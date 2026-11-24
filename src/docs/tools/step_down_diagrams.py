#!/usr/bin/env python3
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# Generate the timeline diagrams used by the asynchronous step-down concurrency architecture page
# (src/docs/arch-disagg-step-down-concurrency.dox). The SVG files are committed; re-run this script
# after changing a diagram:
#
#     python3 src/docs/tools/step_down_diagrams.py
#
# Each diagram draws one lane per thread over a left-to-right time axis. Outlined groups mark the
# public API call that runs the steps inside them.

import html
import os

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "images")

FONT = "Helvetica, Arial, sans-serif"
MONO = "Menlo, Consolas, monospace"
BOX = {
    # style: (fill, stroke, text)
    "plain": ("#ffffff", "#333333", "#111111"),
    "locked": ("#1f4fd8", "#1238a8", "#ffffff"),
    "good": ("#e3f6e5", "#2e8b3e", "#14531f"),
    "note": ("#fff4d6", "#c79a00", "#5c4600"),
}
LOCK = {
    # kind: (fill, stroke)
    "stepdown": ("#f6c9c9", "#c46b6b"),
    "schema": ("#d9c8f2", "#7e5bb5"),
}
ARROW = {
    # style: (color, dash, marker)
    "hb": ("#111111", "", "arrow-hb"),
    "vis": ("#1f4fd8", "5 4", "arrow-vis"),
    "bad": ("#c62828", "", "arrow-bad"),
    "nohb": ("#8a8a8a", "2 4", None),
}


class Diagram:
    def __init__(self, title, lanes, t_max, width=1200, x0=170, lane_h=98, gap=46, top=44):
        self.title = title
        self.lanes = lanes
        self.t_max = t_max
        self.width = width
        self.x0 = x0
        self.top = top
        self.items = []
        self.lane_y = {}
        y = top
        for key, _ in lanes:
            self.lane_y[key] = (y, lane_h)
            y += lane_h + gap
        self.height = y - gap + 40

    def x(self, t):
        return self.x0 + (self.width - self.x0 - 24) * t / self.t_max

    @staticmethod
    def text(cx, cy, text, color, size=12.0, weight="normal", anchor="middle", fit=None,
             font=FONT):
        lines = text.split("\n")
        if fit:
            longest = max(len(line) for line in lines)
            size = max(8.5, min(size, (fit - 8) / (longest * 0.56)))
        first = cy - (len(lines) - 1) * (size + 2) / 2
        spans = "".join(
            f'<tspan x="{cx:.1f}" y="{first + i * (size + 2):.1f}">{html.escape(line)}</tspan>'
            for i, line in enumerate(lines))
        return (f'<text font-family="{font}" font-size="{size:.1f}" font-weight="{weight}" '
                f'fill="{color}" text-anchor="{anchor}" dominant-baseline="middle">{spans}</text>')

    def _box_y(self, lane):
        y, h = self.lane_y[lane]
        # Leave a strip at the top for API call labels and one at the bottom for lock bands.
        return y + 25, h - 47

    def api(self, lane, t0, t1, call):
        """Outline the steps a public API call runs; call before drawing the steps inside it."""
        y, h = self.lane_y[lane]
        x0, x1 = self.x(t0), self.x(t1)
        s = (f'<rect x="{x0:.1f}" y="{y + 3}" width="{x1 - x0:.1f}" height="{h - 6}" rx="7" '
             f'fill="#e9edf5" stroke="#8592ab" stroke-width="1.2"/>')
        # Monospace glyphs are about 0.62 em wide; shrink long calls to fit their group.
        size = max(8.0, min(11.0, (x1 - x0 - 12) / (len(call) * 0.62)))
        s += self.text(x0 + 7, y + 14, call, "#2f3b55", size=size, weight="bold", anchor="start",
                       font=MONO)
        self.items.append((t0, s))

    def box(self, lane, t0, t1, text, style="plain"):
        by, bh = self._box_y(lane)
        fill, stroke, color = BOX[style]
        x0, x1 = self.x(t0), self.x(t1)
        s = (f'<rect x="{x0:.1f}" y="{by}" width="{x1 - x0:.1f}" height="{bh}" rx="5" '
             f'fill="{fill}" stroke="{stroke}" stroke-width="1.4"/>')
        s += self.text((x0 + x1) / 2, by + bh / 2, text, color, size=12, fit=x1 - x0)
        self.items.append((t0, s))

    def lock(self, lane, t0, t1, text, kind="stepdown"):
        y, h = self.lane_y[lane]
        fill, stroke = LOCK[kind]
        x0, x1 = self.x(t0), self.x(t1)
        s = (f'<rect x="{x0:.1f}" y="{y + h - 18}" width="{x1 - x0:.1f}" height="13" rx="3" '
             f'fill="{fill}" stroke="{stroke}" stroke-width="1"/>')
        s += self.text((x0 + x1) / 2, y + h - 11.2, text, "#3a2a2a", size=10.5, fit=x1 - x0)
        self.items.append((t0, s))

    def wait(self, lane, t0, t1, text):
        by, bh = self._box_y(lane)
        x0, x1 = self.x(t0), self.x(t1)
        s = (f'<rect x="{x0:.1f}" y="{by}" width="{x1 - x0:.1f}" height="{bh}" rx="5" '
             f'fill="url(#hatch)" stroke="#888888" stroke-dasharray="4 3" stroke-width="1.2"/>')
        tw = min(len(text) * 6.6 + 10, x1 - x0 - 6)
        s += (f'<rect x="{(x0 + x1) / 2 - tw / 2:.1f}" y="{by + bh / 2 - 9}" width="{tw:.1f}" '
              f'height="18" rx="3" fill="#ffffff" opacity="0.9"/>')
        s += self.text((x0 + x1) / 2, by + bh / 2, text, "#444444", size=11.5, weight="bold",
                       fit=x1 - x0 - 6)
        self.items.append((t0, s))

    def arrow(self, lane_a, ta, lane_b, tb, style="hb", label=None, leg=0.5):
        """Draw an arrow between lanes; leg is the fraction of the vertical distance at which the
        horizontal segment runs, so it can be placed in an empty gap between lanes."""
        ya, ha = self.lane_y[lane_a]
        yb, hb = self.lane_y[lane_b]
        xa, xb = self.x(ta), self.x(tb)
        down = yb > ya
        y0 = ya + ha if down else ya
        y1 = yb if down else yb + hb
        ym = y0 + (y1 - y0) * leg
        color, dash, marker = ARROW[style]
        attrs = f' stroke-dasharray="{dash}"' if dash else ""
        attrs += f' marker-end="url(#{marker})"' if marker else ""
        y_tip = y1 - (3 if down else -3) if marker else y1
        s = (f'<path d="M{xa:.1f},{y0:.1f} L{xa:.1f},{ym:.1f} L{xb:.1f},{ym:.1f} '
             f'L{xb:.1f},{y_tip:.1f}" fill="none" stroke="{color}" stroke-width="1.6"{attrs}/>')
        if label:
            if abs(xb - xa) > 8 * len(label):
                lw = len(label) * 6.6 + 10
                s += (f'<rect x="{(xa + xb) / 2 - lw / 2:.1f}" y="{ym - 8}" width="{lw:.1f}" '
                      f'height="16" fill="#ffffff"/>')
                s += self.text((xa + xb) / 2, ym, label, color, size=11, weight="bold")
            else:
                s += self.text(max(xa, xb) + 8, ym, label, color, size=11, weight="bold",
                               anchor="start")
        self.items.append((max(ta, tb), s))

    def svg(self):
        out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.width} {self.height}" '
               f'width="{self.width}" height="{self.height}" role="img">',
               f'<title>{html.escape(self.title)}</title>',
               '<defs><pattern id="hatch" width="7" height="7" patternUnits="userSpaceOnUse" '
               'patternTransform="rotate(45)"><rect width="7" height="7" fill="#f2f2f2"/>'
               '<line x1="0" y1="0" x2="0" y2="7" stroke="#cfcfcf" stroke-width="3"/></pattern>']
        for color, _, marker in ARROW.values():
            if marker:
                out.append(f'<marker id="{marker}" viewBox="0 0 10 10" refX="9" refY="5" '
                           f'markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
                           f'<path d="M0,0 L10,5 L0,10 z" fill="{color}"/></marker>')
        out.append('</defs>')
        out.append(f'<rect width="{self.width}" height="{self.height}" fill="#ffffff"/>')
        out.append(self.text(16, 22, self.title, "#111111", size=15.5, weight="bold",
                             anchor="start"))
        for key, label in self.lanes:
            y, h = self.lane_y[key]
            out.append(f'<rect x="8" y="{y}" width="{self.width - 16}" height="{h}" rx="8" '
                       f'fill="#f5f6f8" stroke="#c9ccd2"/>')
            out.append(self.text(18, y + h / 2, label, "#222222", size=13, weight="bold",
                                 anchor="start"))
        ya = self.height - 22
        out.append(f'<line x1="{self.x0}" y1="{ya}" x2="{self.width - 14}" y2="{ya}" '
                   f'stroke="#999999" marker-end="url(#arrow-hb)"/>')
        out.append(self.text(self.width - 20, ya + 12, "time", "#777777", size=11, anchor="end"))
        for _, s in sorted(self.items, key=lambda item: item[0]):
            out.append(f'<g>{s}</g>')
        out.append('</svg>')
        return "\n".join(out) + "\n"


def write(name, diagram):
    with open(os.path.join(OUT, name), "w") as f:
        f.write(diagram.svg())


# The leg fraction that places a horizontal arrow segment in the far gap when an arrow crosses one
# intermediate lane.
FAR_GAP = 0.86
RECONFIGURE = "reconfigure(role=follower)"
SET_TS = "set_timestamp(step_down_timestamp=N)"


def legend():
    d = Diagram("How to read the step-down diagrams", [("a", "Steps"), ("b", "Arrows")], 12.0,
                gap=14, lane_h=98)
    d.height -= 30
    d.api("a", 0.0, 3.8, "API call")
    d.box("a", 0.1, 1.7, "step")
    d.box("a", 1.9, 3.7, "step under the\nstep-down lock", "locked")
    d.lock("a", 1.9, 3.7, "lock held")
    d.wait("a", 4.0, 6.2, "suspended or waiting")
    d.box("a", 6.4, 9.0, "intended outcome", "good")
    d.box("a", 9.2, 12.0, "rollback or retry", "note")
    y, h = d.lane_y["b"]
    entries = [("hb", "lock hand-off (happens-before)"), ("vis", "guaranteed visible"),
               ("bad", "ordering that causes the problem"), ("nohb", "no ordering")]
    for i, (style, label) in enumerate(entries):
        color, dash, marker = ARROW[style]
        x0 = d.x(i * 3.0)
        attrs = f' stroke-dasharray="{dash}"' if dash else ""
        attrs += f' marker-end="url(#{marker})"' if marker else ""
        s = (f'<line x1="{x0:.1f}" y1="{y + h / 2}" x2="{x0 + 50:.1f}" y2="{y + h / 2}" '
             f'stroke="{color}" stroke-width="1.6"{attrs}/>')
        s += d.text(x0 + 58, y + h / 2, label, "#222222", size=11.5, anchor="start")
        d.items.append((0, s))
    svg = d.svg()
    # The legend has no time axis.
    svg = svg.replace(f'<line x1="{d.x0}" y1="{d.height - 22}"', '<line visibility="hidden" '
                      f'x1="{d.x0}" y1="{d.height - 22}"')
    svg = svg.replace('fill="#777777" text-anchor="end"', 'fill="none" text-anchor="end"')
    with open(os.path.join(OUT, "step-down-legend.svg"), "w") as f:
        f.write(svg)


def clear_after_publish():
    lanes = [("rc", "Step-down\nreconfiguration"), ("rd", "Reader\ntransaction")]
    d = Diagram("Publish the follower role, then clear the step-down timestamp under the lock",
                lanes, 10.0)
    d.api("rc", 0.0, 4.4, RECONFIGURE)
    d.box("rc", 0.6, 2.4, "publish role = follower\n(release store)")
    d.box("rc", 2.6, 4.2, "clear step-down\ntimestamp", "locked")
    d.lock("rc", 2.6, 4.2, "step-down write lock")
    d.api("rd", 0.0, 6.0, "begin_transaction")
    d.box("rd", 0.2, 1.6, "acquire snapshot")
    d.wait("rd", 1.8, 4.2, "suspended")
    d.box("rd", 4.4, 5.8, "read step-down ts\n= cleared", "locked")
    d.lock("rd", 4.4, 5.8, "step-down read lock")
    d.api("rd", 6.2, 10.0, "search")
    d.box("rd", 6.4, 8.0, "load role (acquire)\n= follower", "good")
    d.box("rd", 8.2, 9.8, "open the\nfollower view", "good")
    d.arrow("rc", 4.2, "rd", 4.4, "hb", label="lock hand-off", leg=0.3)
    d.arrow("rc", 1.5, "rd", 7.2, "vis", label="guaranteed visible", leg=0.7)
    write("step-down-clear-after-publish.svg", d)


def eviction():
    lanes = [("rc", "Step-down\nreconfiguration"), ("ev", "Eviction\nthread"), ("rd", "Reader")]
    d = Diagram("Drain eviction for the tree before marking its data handle outdated", lanes, 12.0)
    d.api("rc", 0.0, 7.8, RECONFIGURE)
    d.box("rc", 0.2, 2.4, "exclusive eviction access:\nstop new eviction, clear\nwalk, drop candidates")
    d.wait("rc", 2.6, 4.2, "waits for eviction")
    d.box("rc", 4.4, 6.0, "mark data handle\noutdated")
    d.box("rc", 6.2, 7.6, "re-enable\neviction")
    d.box("ev", 0.0, 4.2, "in-flight eviction of a page\nfrom this tree")
    d.wait("ev", 4.4, 7.6, "no eviction of this tree")
    d.box("ev", 7.8, 10.2, "retain pages that cannot be\nread back from shared storage")
    d.box("ev", 10.4, 12.0, "discard remaining\npages, no reconcile", "good")
    d.api("rd", 0.0, 8.8, "search")
    d.box("rd", 0.2, 8.6, "reader keeps using the old stable B-tree")
    d.api("rd", 9.0, 10.2, "close")
    d.box("rd", 9.2, 10.0, "release\nhandle")
    d.arrow("ev", 4.2, "rc", 4.4, "hb", label="drained")
    d.arrow("rd", 10.0, "ev", 10.4, "hb", label="last user gone")
    write("step-down-eviction-drain.svg", d)


def schema():
    lanes = [("st", "Setting the\nstep-down ts"), ("sc", "Schema\noperations")]
    d = Diagram("Table creation takes the schema lock first: it creates both constituents", lanes,
                8.0)
    d.api("sc", 0.0, 3.6, "create")
    d.box("sc", 0.2, 3.4, "create layered table:\nstable + ingest")
    d.lock("sc", 0.2, 3.4, "schema lock", "schema")
    d.api("st", 0.6, 6.0, SET_TS)
    d.wait("st", 0.8, 3.4, "waits for schema lock")
    d.box("st", 3.6, 5.8, "set step-down ts", "locked")
    d.lock("st", 3.6, 5.8, "schema lock", "schema")
    d.arrow("sc", 3.4, "st", 3.6, "hb", label="lock hand-off")
    write("step-down-schema-create-first.svg", d)

    d = Diagram("Setting the step-down timestamp takes the schema lock first: ingest only", lanes,
                11.0)
    d.api("st", 0.0, 3.4, SET_TS)
    d.box("st", 0.2, 2.4, "set step-down ts", "locked")
    d.lock("st", 0.2, 2.4, "schema lock", "schema")
    d.api("sc", 0.4, 5.2, "create")
    d.wait("sc", 0.6, 2.4, "waits")
    d.box("sc", 2.6, 5.0, "create layered table:\ningest only")
    d.lock("sc", 2.6, 5.0, "schema lock", "schema")
    d.api("sc", 5.4, 8.6, "open_cursor")
    d.box("sc", 5.6, 8.4, "open table: ts set, no stable:\nset step-down-created flag")
    d.lock("sc", 5.6, 8.4, "schema lock", "schema")
    d.api("sc", 8.8, 11.0, "search")
    d.box("sc", 9.0, 10.8, "skip the absent\nstable table", "good")
    d.arrow("st", 2.4, "sc", 2.6, "hb", label="lock hand-off")
    write("step-down-schema-set-first.svg", d)


def straddler_commit_check():
    lanes = [("a", "Writer A"), ("st", "Setting the\nstep-down ts"), ("c", "Writer C")]
    d = Diagram("The locked commit-time check decides which writers straddle the step-down "
                "timestamp", lanes, 16.0)
    d.api("st", 4.6, 8.6, SET_TS)
    d.box("st", 4.7, 8.5, "set step-down ts", "locked")
    d.lock("st", 4.7, 8.5, "step-down write lock")
    d.api("st", 12.4, 16.0, "checkpoint")
    d.box("st", 12.5, 15.9, "concluding checkpoint after\nallowed commits finish")
    for lane, t in (("a", 0.0), ("c", 0.6)):
        d.api(lane, t, t + 1.8, "begin_transaction")
        d.box(lane, t + 0.1, t + 1.7, "step-down\nts not set", "locked")
        d.lock(lane, t + 0.1, t + 1.7, "read lock")
    d.api("a", 2.0, 3.0, "insert")
    d.box("a", 2.1, 2.9, "write\nstable")
    d.api("a", 3.2, 11.0, "commit_transaction")
    d.box("a", 3.3, 4.5, "step-down\nts not set", "locked")
    d.lock("a", 3.3, 4.5, "read lock")
    d.box("a", 4.7, 10.9, "rest of commit; may finish after the set (commit ts <= N)", "good")
    d.wait("c", 2.6, 6.8, "suspended")
    d.api("c", 7.0, 11.0, "insert")
    d.box("c", 7.1, 9.3, "check without lock\ncan miss the set")
    d.box("c", 9.5, 10.9, "write\nstable")
    d.api("c", 11.2, 15.0, "commit_transaction")
    d.box("c", 11.3, 12.5, "step-down\nts set", "locked")
    d.lock("c", 11.3, 12.5, "read lock")
    d.box("c", 12.7, 14.9, "WT_ROLLBACK", "note")
    d.arrow("a", 4.5, "st", 4.7, "hb", label="lock hand-off")
    d.arrow("a", 10.9, "st", 12.5, "hb", label="app waits")
    d.arrow("st", 8.5, "c", 11.3, "hb", label="lock hand-off")
    write("step-down-straddler-commit-check.svg", d)


def cross_table_conflicts():
    lanes = [("t1", "Writer T1\n(stable)"), ("st", "Setting the\nstep-down ts"),
             ("t2", "Writer T2\n(ingest)")]
    d = Diagram("Legacy mode: the lock chain makes the stable writer's update visible", lanes,
                15.0)
    d.api("t1", 0.0, 1.8, "begin_transaction")
    d.box("t1", 0.1, 1.7, "step-down\nts not set", "locked")
    d.api("t1", 2.0, 3.8, "insert")
    d.box("t1", 2.1, 3.7, "write K\n(publish update)")
    d.api("t1", 4.0, 8.4, "commit_transaction")
    d.box("t1", 4.1, 5.3, "step-down\nts not set", "locked")
    d.lock("t1", 4.1, 5.3, "read lock")
    d.box("t1", 5.5, 8.3, "rest of commit")
    d.api("st", 5.5, 9.0, SET_TS)
    d.box("st", 5.6, 8.9, "set step-down ts", "locked")
    d.lock("st", 5.6, 8.9, "write lock")
    d.api("t2", 9.2, 12.0, "begin_transaction")
    d.box("t2", 9.3, 10.5, "acquire\nsnapshot")
    d.box("t2", 10.7, 11.9, "step-down\nts set", "locked")
    d.lock("t2", 10.7, 11.9, "read lock")
    d.api("t2", 12.2, 15.0, "insert")
    d.box("t2", 12.3, 13.6, "write K: check\nstable too")
    d.box("t2", 13.8, 14.9, "conflict\nwith T1", "good")
    d.arrow("t1", 5.3, "st", 5.6, "hb", label="lock hand-off")
    d.arrow("st", 8.9, "t2", 10.7, "hb", label="lock hand-off", leg=0.25)
    d.arrow("t1", 2.9, "t2", 13.0, "vis", label="published update visible", leg=0.94)
    write("step-down-conflict-lock-chain.svg", d)

    d = Diagram("Legacy mode: the ingest writer's snapshot can exclude the stable writer", lanes,
                16.0)
    d.api("t1", 0.0, 1.8, "begin_transaction")
    d.box("t1", 0.1, 1.7, "step-down\nts not set", "locked")
    d.api("t1", 2.0, 3.4, "insert")
    d.box("t1", 2.1, 3.3, "write K")
    d.api("t1", 3.6, 12.0, "commit_transaction")
    d.box("t1", 3.7, 4.9, "step-down\nts not set", "locked")
    d.lock("t1", 3.7, 4.9, "read lock")
    d.wait("t1", 5.1, 9.4, "suspended before commit completes")
    d.box("t1", 9.6, 11.9, "commit completes")
    d.api("st", 5.2, 8.8, SET_TS)
    d.box("st", 5.3, 8.7, "set step-down ts", "locked")
    d.lock("st", 5.3, 8.7, "write lock")
    d.api("t2", 2.6, 10.4, "begin_transaction")
    d.box("t2", 2.7, 4.6, "acquire snapshot:\nexcludes T1")
    d.wait("t2", 4.8, 8.8, "suspended")
    d.box("t2", 9.0, 10.3, "step-down\nts set", "locked")
    d.lock("t2", 9.0, 10.3, "read lock")
    d.api("t2", 10.6, 16.0, "insert")
    d.box("t2", 10.7, 11.9, "write K")
    d.box("t2", 12.1, 15.9, "explicit stable check finds\nT1 update a snapshot read misses",
          "good")
    d.arrow("t1", 4.9, "st", 5.3, "hb", label="lock hand-off", leg=0.75)
    d.arrow("st", 8.7, "t2", 9.0, "hb", label="lock hand-off")
    d.arrow("t2", 3.6, "t1", 10.7, "bad", label="snapshot precedes T1's commit", leg=0.94)
    write("step-down-conflict-snapshot.svg", d)


def straddler_vs_ingest():
    lanes = [("t1", "Straddler T1"), ("st", "Setting the\nstep-down ts"),
             ("t2", "Writer T2\n(ingest)")]
    d = Diagram("A straddler and an ingest writer can both write key K; the straddler cannot "
                "commit", lanes, 15.0)
    d.api("t1", 0.0, 1.8, "begin_transaction")
    d.box("t1", 0.1, 1.7, "step-down\nts not set", "locked")
    d.wait("t1", 2.0, 5.0, "suspended")
    d.api("t1", 5.2, 9.6, "insert")
    d.box("t1", 5.3, 7.3, "check without lock\ncan miss the set")
    d.box("t1", 7.5, 9.5, "write K to stable\n(uncommitted)")
    d.api("t1", 9.8, 15.0, "commit_transaction")
    d.box("t1", 9.9, 11.1, "step-down\nts set", "locked")
    d.lock("t1", 9.9, 11.1, "read lock")
    d.box("t1", 11.3, 14.9, "WT_ROLLBACK:\nupdates aborted", "note")
    d.api("st", 1.9, 5.0, SET_TS)
    d.box("st", 2.0, 4.9, "set step-down ts", "locked")
    d.lock("st", 2.0, 4.9, "write lock")
    d.api("t2", 5.2, 7.0, "begin_transaction")
    d.box("t2", 5.3, 6.9, "step-down\nts set", "locked")
    d.lock("t2", 5.3, 6.9, "read lock")
    d.api("t2", 7.2, 12.0, "insert")
    d.box("t2", 7.3, 8.9, "write K to ingest")
    d.box("t2", 9.1, 11.9, "may see T1's update and\nroll back too (safe)", "note")
    d.arrow("st", 4.9, "t2", 5.3, "hb", label="lock hand-off")
    d.arrow("st", 4.9, "t1", 10.3, "hb", label="lock hand-off", leg=0.4)
    d.arrow("t1", 8.5, "t2", 8.1, "nohb", label="no ordering between the writes", leg=FAR_GAP)
    write("step-down-straddler-vs-ingest.svg", d)


def begin_order():
    lanes = [("st", "Setting the\nstep-down ts"), ("w", "Writer W\n(post-N)"),
             ("rd", "Reader R")]
    d = Diagram("Transaction begin acquires the snapshot, then reads the step-down timestamp",
                lanes, 14.0)
    d.api("st", 0.0, 3.6, SET_TS)
    d.box("st", 0.1, 3.5, "set step-down ts", "locked")
    d.lock("st", 0.1, 3.5, "write lock")
    d.api("w", 3.8, 5.6, "begin_transaction")
    d.box("w", 3.9, 5.5, "step-down\nts set", "locked")
    d.lock("w", 3.9, 5.5, "read lock")
    d.api("w", 5.8, 7.0, "insert")
    d.box("w", 5.9, 6.9, "write")
    d.api("w", 7.2, 9.0, "commit_transaction")
    d.box("w", 7.3, 8.9, "commit")
    d.api("rd", 9.2, 12.2, "begin_transaction")
    d.box("rd", 9.3, 10.7, "acquire snapshot:\nincludes W")
    d.box("rd", 10.9, 12.1, "step-down\nts set", "locked")
    d.lock("rd", 10.9, 12.1, "read lock")
    d.api("rd", 12.4, 14.0, "search")
    d.box("rd", 12.5, 13.9, "consult the\ningest table", "good")
    d.arrow("st", 3.5, "w", 3.9, "hb", label="lock hand-off")
    d.arrow("w", 8.9, "rd", 9.3, "vis", label="W in snapshot", leg=0.25)
    d.arrow("st", 1.8, "rd", 11.5, "hb", label="so R sees ts set", leg=0.94)
    write("step-down-begin-snapshot-first.svg", d)


def role_generation():
    lanes = [("rc", "Step-down\nreconfiguration"), ("a", "Reader A\n(no read ts)"),
             ("b", "Reader B\n(no read ts)"), ("c", "Transaction C\nbeginning")]
    d = Diagram("Role generation: readers without a read timestamp detect the role change", lanes,
                13.0)
    d.api("rc", 0.0, 7.0, RECONFIGURE)
    d.box("rc", 0.2, 2.0, "advance role gen\ng -> g+1")
    d.wait("rc", 2.2, 4.6, "not yet published")
    d.box("rc", 4.8, 6.8, "publish role = follower\n(release store)")
    d.api("a", 0.0, 1.6, "begin_transaction")
    d.box("a", 0.1, 1.5, "snapshot:\nleader, g")
    d.api("a", 2.4, 5.8, "search")
    d.box("a", 2.5, 4.0, "first stable open:\nleader, loads g+1")
    d.box("a", 4.2, 5.7, "can detect new gen:\nWT_ROLLBACK", "note")
    d.api("b", 0.0, 1.6, "begin_transaction")
    d.box("b", 0.1, 1.5, "snapshot:\nleader, g")
    d.api("b", 7.2, 11.4, "search")
    d.box("b", 7.3, 9.3, "load role (acquire) =\nfollower: reopen stable")
    d.box("b", 9.5, 11.3, "g+1 guaranteed:\nWT_ROLLBACK", "note")
    d.api("c", 3.0, 12.6, "begin_transaction")
    d.box("c", 3.1, 5.6, "snapshot scan")
    d.box("c", 5.8, 7.6, "recheck: role and\ngen changed")
    d.box("c", 7.8, 9.4, "rebuild\nsnapshot", "note")
    d.box("c", 9.6, 12.5, "snapshot:\nfollower, g+1", "good")
    d.arrow("rc", 6.0, "b", 8.3, "vis", label="release -> acquire", leg=FAR_GAP)
    write("step-down-role-generation.svg", d)


def created_flag():
    lanes = [("l", "Later\ncursor op"), ("rc", "Step-down\nreconfiguration"),
             ("cu", "Racing\ncursor op")]
    d = Diagram("The step-down-created flag is cleared before the follower role is published",
                lanes, 13.6)
    d.api("rc", 0.0, 9.4, RECONFIGURE)
    d.box("rc", 0.1, 1.3, "take schema\nlock")
    d.lock("rc", 0.7, 9.3, "schema lock held by the step-down reconfiguration", "schema")
    d.box("rc", 1.5, 3.1, "clear step-down-\ncreated flag")
    d.box("rc", 4.6, 6.6, "publish role = follower\n(release store)")
    d.box("rc", 8.1, 9.3, "release\nschema lock")
    d.api("cu", 0.3, 13.6, "search")
    d.box("cu", 0.4, 1.8, "load role\n= leader")
    d.box("cu", 3.3, 4.5, "load flag\n= cleared")
    d.box("cu", 4.7, 6.0, "open stable:\nfresh open")
    d.wait("cu", 6.2, 9.3, "waits for schema lock")
    d.box("cu", 9.5, 11.0, "observe role\n= follower")
    d.lock("cu", 9.5, 11.0, "schema lock", "schema")
    d.box("cu", 11.2, 13.5, "checkpoint view: missing\ntable tolerated", "good")
    d.api("l", 7.0, 13.6, "search")
    d.box("l", 7.1, 8.9, "load role (acquire)\n= follower")
    d.box("l", 9.1, 10.5, "load flag\n= cleared")
    d.box("l", 10.7, 13.5, "checkpoint view: missing\ntable tolerated", "good")
    d.arrow("rc", 4.8, "l", 8.0, "vis", label="release -> acquire", leg=0.7)
    d.arrow("rc", 2.3, "l", 9.8, "vis", label="cleared before publish", leg=0.3)
    d.arrow("rc", 9.3, "cu", 9.5, "hb", label="lock hand-off")
    write("step-down-created-flag.svg", d)


def cursor_open_race():
    lanes = [("rc", "Step-down\nreconfiguration"), ("cu", "Cursor operation")]
    d = Diagram("Reconfiguration takes the schema lock first: the fresh open waits, then retries",
                lanes, 13.0)
    d.api("rc", 0.0, 7.6, RECONFIGURE)
    d.box("rc", 0.1, 1.3, "take schema\nlock")
    d.lock("rc", 0.7, 7.5, "schema lock held by the step-down reconfiguration", "schema")
    d.box("rc", 1.5, 3.2, "mark stable data\nhandle outdated")
    d.box("rc", 3.4, 5.2, "publish role\n= follower")
    d.box("rc", 6.3, 7.5, "release\nschema lock")
    d.api("cu", 1.4, 13.0, "search")
    d.box("cu", 1.5, 2.9, "load role\n= leader")
    d.box("cu", 3.3, 5.1, "lookup skips outdated\nhandle: fresh open")
    d.wait("cu", 5.3, 7.5, "waits for schema lock")
    d.box("cu", 7.7, 9.2, "observe role\n= follower")
    d.lock("cu", 7.7, 9.2, "schema lock", "schema")
    d.box("cu", 9.4, 10.9, "live open refused:\nretry operation", "note")
    d.box("cu", 11.1, 12.9, "open checkpoint\nview", "good")
    d.arrow("rc", 7.5, "cu", 7.7, "hb", label="lock hand-off")
    write("step-down-cursor-open-reconfig-first.svg", d)

    d = Diagram("The cursor takes the schema lock first: its live view is included in the walk",
                lanes, 13.0)
    d.api("cu", 0.0, 3.4, "search")
    d.box("cu", 0.1, 1.4, "load role\n= leader")
    d.box("cu", 1.6, 3.3, "fresh open of\nlive B-tree view")
    d.lock("cu", 1.6, 3.3, "schema lock", "schema")
    d.api("cu", 3.6, 10.8, "search / next")
    d.box("cu", 3.7, 10.7, "existing cursors keep reading the old stable B-tree "
          "(protected by eviction synchronization)", "good")
    d.api("cu", 11.0, 13.0, "search")
    d.box("cu", 11.1, 12.9, "later reopen:\ncheckpoint view", "good")
    d.api("rc", 0.6, 10.0, RECONFIGURE)
    d.wait("rc", 0.7, 3.3, "waits for schema lock")
    d.lock("rc", 3.5, 9.9, "schema lock held by the step-down reconfiguration", "schema")
    d.box("rc", 3.5, 5.7, "mark this data handle\noutdated (eviction drained)")
    d.box("rc", 5.9, 7.6, "publish role\n= follower")
    d.box("rc", 8.7, 9.9, "release\nschema lock")
    d.arrow("cu", 3.3, "rc", 3.5, "hb", label="lock hand-off")
    write("step-down-cursor-open-cursor-first.svg", d)


if __name__ == "__main__":
    legend()
    clear_after_publish()
    eviction()
    schema()
    straddler_commit_check()
    cross_table_conflicts()
    straddler_vs_ingest()
    begin_order()
    role_generation()
    created_flag()
    cursor_open_race()
