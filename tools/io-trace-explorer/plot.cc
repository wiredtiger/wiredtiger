/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <cairomm/context.h>
#include <gtkmm.h>

#include <algorithm>
#include <thread>
#include <vector>

#include "plot.h"
#include "trace.h"
#include "util.h"

/*
 * Plot::Plot --
 *     Initialize a new Plot object.
 */
Plot::Plot(PlotGroup &group, const Trace &trace)
    : m_group(group), m_trace(trace), m_margin_top(20), m_margin_bottom(30), m_margin_left(120),
      m_margin_right(10)
{
    m_group.add(*this);
    m_plot_tool = PlotTool::NONE;
    set_size_request(300, m_margin_top);

    /* Set up event handlers. */

    set_draw_func(sigc::mem_fun(*this, &Plot::on_draw));

    GtkGesture *drag = gtk_gesture_drag_new();
    gtk_gesture_single_set_button(GTK_GESTURE_SINGLE(drag), GDK_BUTTON_PRIMARY);
    gtk_widget_add_controller(((Gtk::Widget *)this)->gobj(), GTK_EVENT_CONTROLLER(drag));
    g_signal_connect(drag, "drag-begin", G_CALLBACK(Plot::static_drag_begin), this);
    g_signal_connect(drag, "drag-update", G_CALLBACK(Plot::static_drag_update), this);
    g_signal_connect(drag, "drag-end", G_CALLBACK(Plot::static_drag_end), this);

    /* Get the min and max data coordinates to initialize the view. */

    const std::vector<TraceOperation> &ops = m_trace.operations();

    bzero(&m_toplevel_view, sizeof(m_toplevel_view));
    m_toplevel_view.min_x = ops[0].timestamp;
    m_toplevel_view.max_x = ops[ops.size() - 1].timestamp;

    double dx = abs(m_toplevel_view.max_x - m_toplevel_view.min_x);
    if (dx < 1e-12) {
        m_toplevel_view.max_x += 0.5;
        m_toplevel_view.min_x -= 0.5;
        if (m_toplevel_view.min_x < 0)
            m_toplevel_view.min_x = 0;
    }

    unsigned long minOffset = LONG_MAX;
    unsigned long maxOffset = 0;

    for (const auto &t : ops) {
        if (t.offset < minOffset)
            minOffset = t.offset;
        if (t.offset + t.length > maxOffset)
            maxOffset = t.offset + t.length;
    }

    m_toplevel_view.min_y = minOffset;
    m_toplevel_view.max_y = maxOffset;

    double dy = abs(m_toplevel_view.max_y - m_toplevel_view.min_y);
    if (dy < 1e-12) {
        m_toplevel_view.max_y += 0.5;
        m_toplevel_view.min_y -= 0.5;
        if (m_toplevel_view.min_y < 0)
            m_toplevel_view.min_y = 0;
    }

    m_view = m_toplevel_view;
}

/*
 * Plot::~Plot --
 *     Destroy the plot.
 */
Plot::~Plot() {}

/*
 * Plot::on_drag_begin --
 *     The event handler for mouse drag begin.
 */
void
Plot::on_drag_begin(GtkGestureDrag *, double x, double y)
{
    int pixbuf_width = get_width() - m_margin_left - m_margin_right;
    int pixbuf_height = get_height() - m_margin_top - m_margin_bottom;

    x = std::max(std::min(x - m_margin_left, (double)pixbuf_width), 0.0);
    y = std::max(std::min(y - m_margin_top, (double)pixbuf_height), 0.0);

    m_drag = true;
    m_drag_horizontal = false;
    m_drag_vertical = false;

    m_drag_start_x = (int)x;
    m_drag_start_y = (int)y;
    m_drag_end_x = m_drag_last_x = m_drag_start_x;
    m_drag_end_y = m_drag_last_y = m_drag_start_y;

    queue_draw();
}

/*
 * Plot::on_drag_update --
 *     The event handler for mouse drag update.
 */
void
Plot::on_drag_update(GtkGestureDrag *, double x, double y)
{
    int pixbuf_width = get_width() - m_margin_left - m_margin_right;
    int pixbuf_height = get_height() - m_margin_top - m_margin_bottom;

    m_drag_end_x = m_drag_start_x + (int)x;
    m_drag_end_y = m_drag_start_y + (int)y;
    m_drag_end_x = std::max(std::min(m_drag_end_x, pixbuf_width), 0);
    m_drag_end_y = std::max(std::min(m_drag_end_y, pixbuf_height), 0);

    if (m_plot_tool == PlotTool::MOVE) {
        double x1 = m_drag_last_x;
        double x2 = m_drag_end_x;
        double y1 = m_drag_last_y;
        double y2 = m_drag_end_y;
        double dx = (m_view.max_x - m_view.min_x) * (x2 - x1) / (double)pixbuf_width;
        double dy = (m_view.max_y - m_view.min_y) * (y2 - y1) / (double)pixbuf_height;

        PlotView view = m_view;
        view.min_x -= dx;
        view.max_x -= dx;
        view.min_y += dy;
        view.max_y += dy;
        set_view(view, true);
    }

    double dx = std::abs(m_drag_start_x - m_drag_end_x);
    double dy = std::abs(m_drag_start_y - m_drag_end_y);
    if (dx >= BTE_PLOT_MIN_DRAG_DISTANCE) {
        if (!m_drag_vertical || (dx / dy > 0.15))
            m_drag_horizontal = true;
    }
    if (dy >= BTE_PLOT_MIN_DRAG_DISTANCE) {
        if (!m_drag_horizontal || (dy / dx > 0.15))
            m_drag_vertical = true;
    }

    m_drag_last_x = m_drag_end_x;
    m_drag_last_y = m_drag_end_y;

    queue_draw();
}

/*
 * Plot::on_drag_end --
 *     The event handler for mouse drag end.
 */
void
Plot::on_drag_end(GtkGestureDrag *gesture, double x, double y)
{
    int pixbuf_width = get_width() - m_margin_left - m_margin_right;
    int pixbuf_height = get_height() - m_margin_top - m_margin_bottom;

    m_drag = false;
    m_drag_end_x = m_drag_start_x + (int)x;
    m_drag_end_y = m_drag_start_y + (int)y;
    m_drag_end_x = std::max(std::min(m_drag_end_x, pixbuf_width), 0);
    m_drag_end_y = std::max(std::min(m_drag_end_y, pixbuf_height), 0);

    double x1 = m_drag_start_x;
    double x2 = m_drag_end_x;
    double y1 = m_drag_start_y;
    double y2 = m_drag_end_y;

    if (x1 > x2)
        std::swap(x1, x2);
    if (y1 < y2)
        std::swap(y1, y2);

    if (m_plot_tool == PlotTool::ZOOM) {
        if (m_drag_horizontal || m_drag_vertical) {
            PlotView view = m_view;
            if (m_drag_horizontal) {
                view.min_x = m_view.view_to_data_x(x1, pixbuf_width);
                view.max_x = m_view.view_to_data_x(x2, pixbuf_width);
            }
            if (m_drag_vertical) {
                view.min_y = m_view.view_to_data_y(y1, pixbuf_height);
                view.max_y = m_view.view_to_data_y(y2, pixbuf_height);
            }

            set_view(view);
        }
    }

    m_drag_last_x = m_drag_end_x;
    m_drag_last_y = m_drag_end_y;

    queue_draw();
}

/*
 * Plot::static_drag_begin --
 *     The event handler for mouse drag begin - the static version for the C bindings.
 */
void
Plot::static_drag_begin(GtkGestureDrag *gesture, double x, double y, Plot *widget)
{
    widget->on_drag_begin(gesture, x, y);
}

/*
 * Plot::static_drag_update --
 *     The event handler for mouse drag update - the static version for the C bindings.
 */
void
Plot::static_drag_update(GtkGestureDrag *gesture, double x, double y, Plot *widget)
{
    widget->on_drag_update(gesture, x, y);
}

/*
 * Plot::static_drag_end --
 *     The event handler for mouse drag end - the static version for the C bindings.
 */
void
Plot::static_drag_end(GtkGestureDrag *gesture, double x, double y, Plot *widget)
{
    widget->on_drag_end(gesture, x, y);
}

/*
 * Plot::view_back --
 *     Go back to the previous view (undo the view change).
 */
void
Plot::view_back()
{
    if (m_view_undo.empty())
        return;

    m_view_redo.push_back(m_view);
    m_view = m_view_undo.back();
    m_view_undo.pop_back();

    queue_draw();
}

/*
 * Plot::view_forward --
 *     Go forward to the view that we had just before the last view undo (redo the view change).
 */
void
Plot::view_forward()
{
    if (m_view_redo.empty())
        return;

    m_view_undo.push_back(m_view);
    m_view = m_view_redo.back();
    m_view_redo.pop_back();

    queue_draw();
}

/*
 * Plot::view_reset --
 *     Reset the view.
 */
void
Plot::view_reset()
{
    m_view_undo.push_back(m_view);
    m_view_redo.clear();
    m_view = m_toplevel_view;
    queue_draw();
}

/*
 * Plot::zoom_in --
 *     Zoom in.
 */
void
Plot::zoom_in()
{
    PlotView view = m_view;
    double z = 0.1;
    double dx = view.max_x - view.min_x;
    double dy = view.max_y - view.min_y;
    view.min_x += dx * z;
    view.max_x -= dx * z;
    view.min_y += dy * z;
    view.max_y -= dy * z;
    set_view(view);
    queue_draw();
}

/*
 * Plot::zoom_out --
 *     Zoom out.
 */
void
Plot::zoom_out()
{
    PlotView view = m_view;
    double z = 0.1;
    double dx = view.max_x - view.min_x;
    double dy = view.max_y - view.min_y;
    view.min_x -= dx / (1 - 2 * z) * z;
    view.max_x += dx / (1 - 2 * z) * z;
    view.min_y -= dy / (1 - 2 * z) * z;
    view.max_y += dy / (1 - 2 * z) * z;
    set_view(view);
    queue_draw();
}

/*
 * Plot::set_view --
 *     Set the viewport. This can be optionally done "in place," i.e., without modifying the
 *  undo/redo stacks.
 */
void
Plot::set_view(const PlotView &view, bool in_place)
{
    if (!in_place) {
        m_view_undo.push_back(m_view);
        m_view_redo.clear();
    }

    m_view = view;
    m_group.view_sync(*this, in_place);
}

/*
 * Plot::view_sync --
 *     Synchronize view from the given source view.
 */
void
Plot::view_sync(const PlotView &source, bool in_place)
{
    if (!in_place) {
        m_view_undo.push_back(m_view);
        m_view_redo.clear();
    }

    PlotView view = m_view;
    view.min_x = source.min_x;
    view.max_x = source.max_x;
    m_view = view;

    queue_draw();
}

/*
 * Plot::render_worker --
 *     Render a part of the plot that is between the start and the end indexes.
 */
int
Plot::render_worker(const std::vector<TraceOperation> &trace, int start, int end)
{
    int count = 0;
    int pixbuf_n_channels = m_pixbuf->get_n_channels();
    int pixbuf_rowstride = m_pixbuf->get_rowstride();
    int pixbuf_width = m_pixbuf->get_width();
    int pixbuf_height = m_pixbuf->get_height();
    guchar *pixels = m_pixbuf->get_pixels();
    size_t capacity = pixbuf_height * pixbuf_rowstride;

    guchar *drawn = (guchar *)malloc(capacity);
    memset(drawn, 0, capacity);

    for (int trace_index = start; trace_index < end; trace_index++) {
        const TraceOperation &t = trace[trace_index];

        int x1 = m_view.data_to_view_x(t.timestamp, pixbuf_width);
        int y1 = m_view.data_to_view_y(t.offset, pixbuf_height);
        int x2 = m_view.data_to_view_x(t.timestamp + std::min(0.00001, t.duration), pixbuf_width);
        int y2 = m_view.data_to_view_y(t.offset + t.length, pixbuf_height);

        if (x2 < 0 || x1 >= pixbuf_width)
            continue;
        if (y1 < 0 || y2 >= pixbuf_width)
            continue;

        x1 = std::max(x1, 0);
        x2 = std::min(x2, pixbuf_width - 1);
        y2 = std::max(y2, 0);
        y1 = std::min(y1, pixbuf_height - 1);

        if (x2 < x1 || y1 < y2)
            continue;

        count++;

        int p = y2 * pixbuf_rowstride + x1 * pixbuf_n_channels;
        assert(p <= capacity);
        if (drawn[p])
            continue;
        drawn[p] = 1;

        unsigned color = t.read ? 0x60c060 : 0x800000;
        for (int y = y2; y <= y1; y++) {
            p = y * pixbuf_rowstride + x1 * pixbuf_n_channels;
            for (int x = x1; x <= x2; x++) {
                unsigned *pointer = (unsigned *)(void *)&pixels[p];
                *pointer = (*pointer & 0xff000000u) | color;
                p += 3;
            }
            assert(p <= capacity);
        }
    }

    free(drawn);
    return count;
}

/*
 * get_axis_unit --
 *     Find a good distance between two tickmarks.
 */
static double
get_axis_unit(int view_range, double data_range, bool bytes = false)
{
    if (data_range < 1e-12)
        return 1;

    double start_unit = 1;
    const int SCALE_NUMS_BYTES[] = {1, 2, 4, 8};
    const int SCALE_NUMS_REG[] = {1, 2, 5, 10};
    const int SCALE_NUMS_COUNT = 4;
    const int *SCALE_NUMS = bytes ? SCALE_NUMS_BYTES : SCALE_NUMS_REG;

    while (true) {
        int i = 0;
        int last = 0;
        double unit = start_unit;

        if (unit * view_range / data_range > 600) {
            start_unit /= bytes ? 8 : 10;
            continue;
        }

        while (unit * view_range / data_range < 100) {
            i = (i + 1) % SCALE_NUMS_COUNT;
            unit = unit * SCALE_NUMS[i];
            if (i > 0)
                unit /= SCALE_NUMS[last];
            last = i;
        }

        return unit;
    }
}

/*
 * Plot::on_draw --
 *     Render the plot.
 */
void
Plot::on_draw(const Cairo::RefPtr<Cairo::Context> &cr, int width, int height)
{
    Cairo::TextExtents extents;

    if (width <= 0 || height <= 0)
        return;

    /* Clear the drawing area. */

    cr->save();
    cr->set_source_rgba(1, 1, 1, 1);
    cr->paint();
    cr->restore();

    /* Title. */

    cr->save();
    cr->get_text_extents(m_trace.name(), extents);
    cr->move_to(4, 4 + extents.height);
    cr->show_text(m_trace.name());
    cr->restore();

    /* Render the data. */

    const std::vector<TraceOperation> &trace = m_trace.operations();

    int pixbuf_width = width - m_margin_left - m_margin_right;
    int pixbuf_height = height - m_margin_top - m_margin_bottom;
    if (pixbuf_width <= 0 || pixbuf_height <= 0 || trace.empty())
        return;

    double start_time = current_time();
    std::atomic<long> count_data(0);
    if (m_pixbuf.get() == nullptr || m_pixbuf->get_width() != pixbuf_width ||
      m_pixbuf->get_height() != height || m_view != m_pixbuf_view) {
        m_pixbuf_view = m_view;
        m_pixbuf = Gdk::Pixbuf::create(Gdk::Colorspace::RGB, false, 8, pixbuf_width, pixbuf_height);
        guchar *pixels = m_pixbuf->get_pixels();
        memset(pixels, 0xff, pixbuf_height * m_pixbuf->get_rowstride());

        int min_data_x = m_view.view_to_data_x(0, pixbuf_width);
        int max_data_x = m_view.view_to_data_x(pixbuf_width, pixbuf_width);
        long min_data_i = std::lower_bound(trace.begin(), trace.end(),
                            TraceOperation::wrap_timestamp(min_data_x * 0.95)) -
          trace.begin();
        long max_data_i = std::upper_bound(trace.begin(), trace.end(),
                            TraceOperation::wrap_timestamp(max_data_x * 1.05)) -
          trace.begin();
        long data_length = max_data_i - min_data_i;

        int num_threads = data_length < 10000 ? 1 : 8;
        std::vector<std::thread> workers;
        for (int i = 0; i < num_threads; i++) {
            int start = min_data_i + i * data_length / num_threads;
            int end = min_data_i + (i + 1) * data_length / num_threads;
            workers.push_back(std::thread([this, &trace, &count_data, start, end]() {
                count_data += render_worker(trace, start, end);
            }));
        }
        std::for_each(workers.begin(), workers.end(), [](std::thread &t) { t.join(); });
    }
    double render_time = current_time() - start_time;
    if (render_time > 0.5) {
        g_warning("Rendering the plot took %.2lf seconds (%.2lf mil. data points)", render_time,
          count_data / 1.0e6);
    }

    cr->save();
    Gdk::Cairo::set_source_pixbuf(cr, m_pixbuf, m_margin_left, m_margin_top);
    cr->paint();
    cr->restore();

    /* Draw the inspection cross-hairs. */

    if (m_drag && m_plot_tool == PlotTool::INSPECT) {
        cr->save();
        cr->set_source_rgba(0.5, 0.5, 0.5, 0.7);

        double x = m_margin_left + m_drag_end_x;
        double y = m_margin_top + m_drag_end_y;

        cr->move_to(m_margin_left, y);
        cr->line_to(m_margin_left + pixbuf_width, y);
        cr->move_to(x, m_margin_top);
        cr->line_to(x, m_margin_top + pixbuf_height);

        cr->stroke();
        cr->restore();
    }

    /* Draw the rectangle for mouse drag. */

    if (m_drag && m_plot_tool == PlotTool::ZOOM) {
        cr->save();
        cr->set_source_rgba(0.0, 0.0, 1.0, 0.7);

        double x1 = m_margin_left + m_drag_start_x;
        double x2 = m_margin_left + m_drag_end_x;
        double y1 = m_margin_top + m_drag_start_y;
        double y2 = m_margin_top + m_drag_end_y;
        if (x1 > x2)
            std::swap(x1, x2);
        if (y1 < y2)
            std::swap(y1, y2);

        if (m_drag_horizontal && !m_drag_vertical) {
            y1 = m_margin_top;
            y2 = m_margin_top + pixbuf_height;
        } else if (!m_drag_horizontal && m_drag_vertical) {
            x1 = m_margin_left;
            x2 = m_margin_left + pixbuf_width;
        }

        cr->rectangle(x1, y1, x2 - x1, y2 - y1);
        cr->fill();
        cr->restore();
    }

    /* Draw the plot axes. */

    cr->save();
    cr->set_source_rgba(0, 0, 0, 1);
    cr->move_to(m_margin_left, m_margin_top + pixbuf_height);
    cr->line_to(m_margin_left + pixbuf_width, m_margin_top + pixbuf_height);
    cr->move_to(m_margin_left, m_margin_top + pixbuf_height);
    cr->line_to(m_margin_left, m_margin_top);
    cr->stroke();
    cr->restore();

    double scale_y = 1048576.0;
    double unit_x = get_axis_unit(pixbuf_width, m_view.max_x - m_view.min_x);
    double unit_y = get_axis_unit(pixbuf_height, (m_view.max_y - m_view.min_y) / scale_y, true);

    cr->save();
    double m = std::floor(m_toplevel_view.min_x / unit_x) * unit_x;
    while (m_view.data_to_view_x(m, pixbuf_width) < pixbuf_width) {
        int p = m_view.data_to_view_x(m, pixbuf_width);
        if (p >= 0) {
            cr->move_to(m_margin_left + p, m_margin_top + pixbuf_height);
            cr->line_to(m_margin_left + p, m_margin_top + pixbuf_height + 4);
            cr->stroke();

            char text[64];
            snprintf(text, sizeof(text), "%.*lf", (int)-std::min(floor(log10(unit_x)), 0.0), m);

            cr->get_text_extents(text, extents);
            cr->move_to(m_margin_left + p - extents.width / 2,
              m_margin_top + pixbuf_height + 8 + extents.height / 2);
            cr->show_text(text);
        }
        m += unit_x;
    }
    cr->restore();

    cr->save();
    m = std::floor(m_toplevel_view.min_y / unit_y / scale_y) * unit_y;
    while (m_view.data_to_view_y(m * scale_y, pixbuf_height) > 0) {
        int p = m_view.data_to_view_y(m * scale_y, pixbuf_height);
        if (p < pixbuf_height) {
            cr->move_to(m_margin_left, m_margin_top + p);
            cr->line_to(m_margin_left - 4, m_margin_top + p);
            cr->stroke();

            char text[64];
            char str_k[32];
            if ((long)m >= 1000) {
                snprintf(text, sizeof(text), "%ld,%03ldM", ((long)m) / 1000, ((long)m) % 1000);
            } else {
                snprintf(text, sizeof(text), "%.0lfM", m);
            }
            if (unit_y < 0.999) {
                snprintf(str_k, sizeof(str_k), " + %dK", (int)round(m * 1024) % 1024);
                strcat(text, str_k);
            }

            cr->get_text_extents(text, extents);
            cr->move_to(m_margin_left - 8 - extents.width, m_margin_top + p + extents.height / 2);
            cr->show_text(text);
        }
        m += unit_y;
    }
    cr->restore();
}

/*
 * PlotGroup::PlotGroup --
 *     Create a new plot group.
 */
PlotGroup::PlotGroup() : m_active_plot(NULL) {}

/*
 * PlotGroup::~PlotGroup --
 *     Destroy the plot group.
 */
PlotGroup::~PlotGroup() {}

/*
 * PlotGroup::add --
 *     Add another plot to the group.
 */
void
PlotGroup::add(Plot &plot)
{
    m_plots.push_back(&plot);
    if (m_active_plot == NULL)
        m_active_plot = &plot;
}

/*
 * PlotGroup::view_back --
 *     Go back to the previous views (undo the view change).
 */
void
PlotGroup::view_back()
{
    for (Plot *p : m_plots)
        p->view_back();
}

/*
 * PlotGroup::view_forward --
 *     Go forward to the views that we had just before the last view undo (redo the view change).
 */
void
PlotGroup::view_forward()
{
    for (Plot *p : m_plots)
        p->view_forward();
}

/*
 * PlotGroup::view_reset --
 *     Reset the view.
 */
void
PlotGroup::view_reset()
{
    for (Plot *p : m_plots)
        p->view_reset();
}

/*
 * PlotGroup::view_reset_x --
 *     Reset just the X axes across all plots.
 */
void
PlotGroup::view_reset_x()
{
    double min_x = INFINITY;
    double max_x = -INFINITY;
    for (Plot *p : m_plots) {
        min_x = std::min(min_x, p->m_view.min_x);
        max_x = std::max(max_x, p->m_view.max_x);
    }
    for (Plot *p : m_plots) {
        p->m_toplevel_view.min_x = p->m_view.min_x = min_x;
        p->m_toplevel_view.max_x = p->m_view.max_x = max_x;
    }
}

/*
 * PlotGroup::view_sync --
 *     Synchronize the views from the given source.
 */
void
PlotGroup::view_sync(Plot &source, bool in_place)
{
    m_active_plot = &source;
    for (Plot *p : m_plots) {
        if (&source != p)
            p->view_sync(source.m_view, in_place);
    }
}
