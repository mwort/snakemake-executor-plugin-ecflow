"""Progress of a deployed suite, per rule and overall.

    python -m snakemake_executor_plugin_ecflow.monitor --suite NAME [--report progress.md]

Run by the admin/monitor task (--ecflow-monitor MINUTES). Everything comes from the
ecFlow server, not from snakemake, which does not run the tasks:
- task states and limits from the suite definition. A rule is the first node under
  main: a rule's family, the task of a rule without wildcards, or a group family.
- runtimes from the state changes in the server log (submitted, active, complete). The
  ones already read are cached (--cache), so the log can be rotated in between. Tasks
  set complete without running (their output was there) count as done, not as runs.

With --meters it also sets the meter of each rule family (--ecflow-addmeter) to the
rule's complete tasks. The tasks count themselves on it as they finish, which loses a
count when two finish at the same moment, and a requeue or rerun is not counted at all.

Times to completion are estimated two ways: from the mean runtime and the limit the
rule's tasks are in (remaining * runtime / concurrency), and from the rate of completed
runs over the last --window hours. Totals only cover the tasks deployed so far, a
checkpoint adds its dependents once it has run.
"""
import argparse
import datetime as dt
import json
import os
import re
import statistics
import subprocess
import textwrap

from pyflow.importer import ecflow

TIME = "%H:%M:%S %d.%m.%Y"
LINE = re.compile(r"^\w+:\[(\d\d:\d\d:\d\d \d\d\.\d\d\.\d{4})\]\s+(\w+): (/\S+)")
STATES = ("complete", "active", "submitted", "queued", "aborted")


def log_time(line: str):
    """Time stamp of a server log line, None for a line without one."""
    m = re.match(r"^\w+:\[([^\]]+)\]", line)
    try:
        return dt.datetime.strptime(m.group(1), TIME).timestamp() if m else None
    except ValueError:
        return None


def new_log_lines(client, since: float = None, n: int = 20000) -> list:
    """The lines of the server log from the time since on (all without it), fetching
    more lines until the first is older than that or the whole log is there."""
    while True:
        lines = client.get_log(n).splitlines()
        first = next((t for t in map(log_time, lines) if t is not None), None)
        if len(lines) < n or (since is not None and (first is None or first <= since)):
            break
        n *= 4
    if since is None:
        return lines
    return [l for l in lines if (log_time(l) or 0) >= since]


def update_runs(runs: dict, done: dict, lines: list, prefix: str) -> float:
    """Add the state changes of the tasks under prefix to runs, {path: [submitted,
    active, complete]} of the current attempt, which a requeue starts over, and to
    done, the same of the last attempt that ran to complete. A task set complete
    without running (by a redeploy that requeues it, too) keeps its last run there.
    Returns the time of the last line."""
    last = None
    for line in lines:
        last = log_time(line) or last
        m = LINE.match(line)
        if not m:
            continue
        t = dt.datetime.strptime(m.group(1), TIME).timestamp()
        state, path = m.group(2), m.group(3)
        if not path.startswith(prefix):
            continue
        run = runs.setdefault(path, [None, None, None])
        if state == "queued":
            run[:] = [None, None, None]
        elif state == "submitted":
            run[:] = [t, None, None]
        elif state == "active":
            run[1:] = [t, None]
        elif state == "complete":
            run[2] = t
            if run[1] is not None:
                done[path] = list(run)
    return last


def concurrency(defs, task) -> tuple:
    """(tasks of this kind that can run at once, the limit that sets it) from the
    task's inlimits, (None, None) without any."""
    best = (None, None)
    for inlimit in task.inlimits:
        node = defs.find_abs_node(inlimit.path_to_node())
        limit = node.find_limit(inlimit.name()) if node is not None else None
        if limit is None or limit.name() is None:
            continue
        n = limit.limit() // max(inlimit.tokens(), 1)
        if best[0] is None or n < best[0]:
            best = (n, limit.name())
    return best


def duration(seconds) -> str:
    if seconds is None:
        return "-"
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds >= 2 * size:
            return f"{seconds / size:.1f}{unit}"
    return f"{seconds:.0f}s"


def summarise(defs, suite: str, runs: dict, now: float, window: float) -> dict:
    """{rule: stats} plus "all", see the module doc."""
    main = defs.find_abs_node(f"/{suite}/main")
    if main is None:
        raise SystemExit(f"/{suite}/main is not on the server.")
    prefix = main.get_abs_node_path() + "/"
    rules = {}
    for task in (n for n in main.get_all_nodes() if isinstance(n, ecflow.Task)):
        path = task.get_abs_node_path()
        rule = path[len(prefix):].split("/")[0]
        r = rules.setdefault(rule, {"tasks": 0, **{s: 0 for s in STATES}, "run": [], "wait": [],
                                    "recent": 0, "first": None, "start": now})
        if r["first"] is None:
            r["first"] = task
        r["tasks"] += 1
        state = str(task.get_state())
        if state in r:
            r[state] += 1
        sub, act, end = runs.get(path, (None, None, None))
        if act is not None and end is not None:
            r["run"].append(end - act)
            r["recent"] += end >= now - window
        if sub is not None and act is not None:
            r["wait"].append(act - sub)
        r["start"] = min(r["start"], sub or act or now)
    out = {}
    for rule, r in rules.items():
        remaining = r["tasks"] - r["complete"]
        conc, limit = concurrency(defs, r.pop("first"))
        mean = statistics.mean(r["run"]) if r["run"] else None
        # completed runs per second, over the window or since the rule's first run
        rate = r["recent"] / max(min(window, now - r["start"]), 1)
        out[rule] = {
            **{k: r[k] for k in ("tasks", *STATES)},
            "done": 100 * r["complete"] / r["tasks"],
            "runs": len(r["run"]),
            "mean_run": mean,
            "median_run": statistics.median(r["run"]) if r["run"] else None,
            "mean_wait": statistics.mean(r["wait"]) if r["wait"] else None,
            "limit": f"{limit} ({conc})" if limit else "-",
            "eta_limit": remaining * mean / conc if remaining and mean and conc else (0 if not remaining else None),
            "eta_rate": remaining / rate if remaining and rate else (0 if not remaining else None),
        }
    total = sum(r["tasks"] for r in out.values())
    complete = sum(r["complete"] for r in out.values())
    recent = sum(r["recent"] for r in rules.values())
    runs_all = [x for r in rules.values() for x in r["run"]]
    etas = [r["eta_limit"] for r in out.values() if r["eta_limit"] is not None]
    since = max(min(window, now - min(r["start"] for r in rules.values())), 1) if rules else 1
    out["all"] = {
        "tasks": total,
        **{s: sum(r[s] for r in out.values()) for s in STATES},
        "done": 100 * complete / total if total else 0,
        "runs": len(runs_all),
        "mean_run": statistics.mean(runs_all) if runs_all else None,
        "median_run": statistics.median(runs_all) if runs_all else None,
        "mean_wait": None,
        "limit": "-",
        # rules run side by side, so the slowest one bounds the whole; at least that
        # while some rules have no runtime yet
        "eta_limit": max(etas, default=None),
        "eta_limit_partial": len(etas) < len(out),
        "eta_rate": (total - complete) / (recent / since) if recent else (0 if total == complete else None),
    }
    return out


def fix_meters(client, defs, suite: str, stats: dict, name: str) -> list:
    """Set the meter of every rule family under main to the rule's complete
    tasks, and its maximum to the rule's tasks, which checkpoints change when
    they add tasks to the family. Returns (rule, old, new) of the meters that
    were off, as "value/max"."""
    main = defs.find_abs_node(f"/{suite}/main")
    fixed = []
    for family in main.nodes:
        if not isinstance(family, ecflow.Family) or family.name() not in stats:
            continue
        meter = next((m for m in family.meters if m.name() == name), None)
        if meter is None:
            continue
        r = stats[family.name()]
        path = family.get_abs_node_path()
        top = max(meter.min() + 1, r["tasks"])
        value = max(meter.min(), min(r["complete"], top))
        if meter.max() != top:
            # ecFlow can only change a meter's value, so it is replaced
            client.alter(path, "delete", "meter", name)
            client.alter(path, "add", "meter", name, f"{meter.min()},{top},{value}")
        elif meter.value() != value:
            client.alter(path, "change", "meter", name, str(value))
        else:
            continue
        fixed.append((family.name(), f"{meter.value()}/{meter.max()}", f"{value}/{top}"))
    return fixed


COLS = ["rule", "tasks", "complete", "done", "active", "submitted", "queued", "aborted",
        "runs", "mean run", "median run", "mean wait", "limit", "eta (limit)", "eta (rate)"]


def rows(stats: dict) -> list:
    return [[rule, r["tasks"], r["complete"], f"{r['done']:.1f}%", r["active"], r["submitted"],
             r["queued"], r["aborted"], r["runs"], duration(r["mean_run"]), duration(r["median_run"]),
             duration(r["mean_wait"]), r["limit"],
             (">" if r.get("eta_limit_partial") else "") + duration(r["eta_limit"]),
             duration(r["eta_rate"])] for rule, r in stats.items()]


def header(suite: str, now: float, window: float) -> str:
    return (f"Progress of {suite} as of {dt.datetime.fromtimestamp(now):%Y-%m-%d %H:%M} (server time). "
            "eta (limit): remaining * mean run / tasks the limit lets run at once; "
            f"eta (rate): remaining / completed runs per hour over the last {window / 3600:g}h "
            "(or since the rule's first run); the overall eta (limit) is the slowest rule's, "
            "> while some rules have no runtime yet. Tasks deployed so far only: checkpoints "
            "add their dependents once they have run.")


def markdown(stats: dict, suite: str, now: float, window: float) -> str:
    body = [[f"**{c}**" if row[0] == "all" and i == 0 else c for i, c in enumerate(row)] for row in rows(stats)]
    lines = [f"# Progress of {suite}", "", header(suite, now, window), "",
             "| " + " | ".join(COLS) + " |", "|" + "---|" * len(COLS)]
    lines += ["| " + " | ".join(map(str, row)) + " |" for row in body]
    return "\n".join(lines) + "\n"


def text(stats: dict, suite: str, now: float, window: float) -> str:
    """The table aligned in plain text, for the job output."""
    body = [list(map(str, row)) for row in rows(stats)]
    widths = [max(len(r[i]) for r in [COLS, *body]) for i in range(len(COLS))]

    def line(row):
        return "  ".join(c.ljust(w) if i == 0 else c.rjust(w) for i, (c, w) in enumerate(zip(row, widths)))

    rule = "-" * len(line(COLS))
    lines = [textwrap.fill(header(suite, now, window), 100), "", line(COLS), rule]
    lines += [line(row) for row in body[:-1]] + [rule, line(body[-1])]
    return "\n".join(lines) + "\n"


def labels(stats: dict) -> dict:
    """The overall numbers, for the monitor task's labels."""
    r = stats["all"]
    slow = max((k for k in stats if k != "all"), key=lambda k: stats[k]["eta_limit"] or 0, default=None)
    partial = ">" if r["eta_limit_partial"] else ""
    return {
        "progress": f"{r['done']:.1f}% done ({r['complete']}/{r['tasks']} tasks)",
        "status": f"{r['active']} active, {r['submitted']} submitted, {r['queued']} queued, {r['aborted']} aborted",
        "time": f"eta {duration(r['eta_rate'])} (rate), {partial}{duration(r['eta_limit'])} (limits"
                f"{', slowest: ' + slow if slow else ''}), mean run {duration(r['mean_run'])}",
    }


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--suite", required=True)
    p.add_argument("--host", default=os.environ.get("ECF_HOST", "localhost"))
    p.add_argument("--port", default=int(os.environ.get("ECF_PORT", 3141)), type=int)
    p.add_argument("--report", help="Also write the table to this markdown file")
    p.add_argument("--json", help="Also write the numbers to this file")
    p.add_argument("--cache", help="Run times read from the log so far (JSON), kept up to date")
    p.add_argument("--window", default=24, type=float, help="Hours of completions the rate is taken over")
    p.add_argument("--meters", nargs="?", const="done", metavar="NAME",
                   help="Set the meters of this name (default: done) on the rule families "
                        "to the rule's complete tasks, and their maximum to its tasks")
    p.add_argument("--labels", action="store_true",
                   help="Set the progress, status and time labels of the ecFlow task this runs in")
    a = p.parse_args(argv)

    client = ecflow.Client(a.host, a.port)
    cache = {"last": None, "runs": {}, "done": {}}
    if a.cache and os.path.exists(a.cache):
        with open(a.cache) as f:
            cached = json.load(f)
        if "done" in cached:  # otherwise of an older version, read the log again
            cache = cached
    lines = new_log_lines(client, cache["last"])
    last = update_runs(cache["runs"], cache["done"], lines, f"/{a.suite}/main/")
    cache["last"] = last or cache["last"]
    if a.cache:
        with open(a.cache + ".tmp", "w") as f:
            json.dump(cache, f)
        os.replace(a.cache + ".tmp", a.cache)

    client.sync_local()
    now = cache["last"] or dt.datetime.now().timestamp()  # server time, as the log
    window = a.window * 3600
    defs = client.get_defs()
    stats = summarise(defs, a.suite, cache["done"], now, window)
    print(text(stats, a.suite, now, window))
    if a.meters:
        for rule, old, new in fix_meters(client, defs, a.suite, stats, a.meters):
            print(f"Meter {a.meters} of {rule} was {old}, set to {new}.")
    if a.report:
        with open(a.report, "w") as f:
            f.write(markdown(stats, a.suite, now, window))
    if a.json:
        with open(a.json, "w") as f:
            json.dump(stats, f, indent=1)
    for name, value in labels(stats).items():
        print(f"{name}: {value}")
        if a.labels:
            # the job's ecflow_client, which knows the task from ECF_NAME, ECF_PASS etc.
            subprocess.run(["ecflow_client", f"--label={name}", value], check=True)


if __name__ == "__main__":
    main()
