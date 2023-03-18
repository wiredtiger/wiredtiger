/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef BLOCK_TRACE_EXPLORER_PLOT_H
#define BLOCK_TRACE_EXPLORER_PLOT_H

#include <glibmm/main.h>
#include <gtkmm.h>
#include <iostream>

#include "trace.h"

/* The minimum distance in pixels for a mouse drag to be recognized as such. */
#define BTE_PLOT_MIN_DRAG_DISTANCE 10

/*
 * PlotView --
 *     A view definition for a Plot; i.e., what is visible within the given viewport.
 */
struct PlotView {
    double min_x;
    double max_x;
    double min_y;
    double max_y;

    /*
     * data_to_view_x --
     *     Convert a data X value to the corresponding viewport coordinate.
     */
    inline int
    data_to_view_x(double x, int width) const
    {
        if (max_x == min_x)
            return 0;
        return (int)(width * (x - min_x) / (max_x - min_x));
    }

    /*
     * data_to_view_t --
     *     Convert a data Y value to the corresponding viewport coordinate.
     */
    inline int
    data_to_view_y(double y, int height) const
    {
        if (max_y == min_y)
            return 0;
        return (int)(height - 1 - height * (y - min_y) / (max_y - min_y));
    }

    /*
     * view_to_data_x --
     *     Convert a viewport X coordinate to the corresponding data value.
     */
    inline double
    view_to_data_x(double x, int width) const
    {
        if (width == 0)
            return min_x;
        return x * (max_x - min_x) / width + min_x;
    }

    /*
     * view_to_data_y --
     *     Convert a viewport Y coordinate to the corresponding data value.
     */
    inline double
    view_to_data_y(double y, int height) const
    {
        if (height == 0)
            return min_y;
        return (height - 1 - y) * (max_y - min_y) / height + min_y;
    }

    /*
     * operator== --
     *     The equality operator.
     */
    inline bool
    operator==(const PlotView &other) const
    {
        return min_x == other.min_x && max_x == other.max_x && min_y == other.min_y &&
          max_y == other.max_y;
    }

    /*
     * operator!= --
     *     The inequality operator.
     */
    inline bool
    operator!=(const PlotView &other) const
    {
        return min_x != other.min_x || max_x != other.max_x || min_y != other.min_y ||
          max_y != other.max_y;
    }
};

/*
 * PlotTool --
 *     A tool for user interaction with the Plot.
 */
enum class PlotTool {
    NONE,
    INSPECT,
    MOVE,
    ZOOM,
};

class PlotGroup;

/*
 * Plot --
 *     The GTK+ component for drawing the plot.
 */
class Plot : public Gtk::DrawingArea {

    friend class PlotGroup;

public:
    Plot(PlotGroup &group, const Trace &trace);
    virtual ~Plot();

    void view_back();
    void view_forward();
    void view_reset();
    void zoom_in();
    void zoom_out();

    /*
     * active_tool --
     *     Get the active tool.
     */
    inline PlotTool
    active_tool()
    {
        return m_plot_tool;
    }

    /*
     * set_active_tool --
     *     Set the active tool.
     */
    inline void
    set_active_tool(PlotTool tool)
    {
        m_plot_tool = tool;
    }

protected:
    void set_view(const PlotView &view, bool in_place = false);

    void on_draw(const Cairo::RefPtr<Cairo::Context> &cr, int width, int height);
    void on_drag_begin(GtkGestureDrag *gesture, double x, double y);
    void on_drag_update(GtkGestureDrag *gesture, double x, double y);
    void on_drag_end(GtkGestureDrag *gesture, double x, double y);

    PlotGroup &m_group;
    const Trace &m_trace;
    PlotTool m_plot_tool;

    bool m_drag;
    bool m_drag_horizontal;
    bool m_drag_vertical;

    int m_drag_start_x;
    int m_drag_start_y;
    int m_drag_last_x;
    int m_drag_last_y;
    int m_drag_end_x;
    int m_drag_end_y;

    PlotView m_toplevel_view;
    PlotView m_view;
    std::vector<PlotView> m_view_undo;
    std::vector<PlotView> m_view_redo;

    Glib::RefPtr<Gdk::Pixbuf> m_pixbuf; /* The pixel buffer for the actual plot. */
    PlotView m_pixbuf_view; /* The view coordinates for checking whether the plot needs to be
                               re-rendered. */

    int m_margin_top;
    int m_margin_bottom;
    int m_margin_left;
    int m_margin_right;

private:
    static void static_drag_begin(GtkGestureDrag *gesture, double x, double y, Plot *widget);
    static void static_drag_update(GtkGestureDrag *gesture, double x, double y, Plot *widget);
    static void static_drag_end(GtkGestureDrag *gesture, double x, double y, Plot *widget);

    int render_worker(const std::vector<TraceOperation> &trace, int start, int end);
    void view_sync(const PlotView &source, bool in_place = false);
};

/*
 * PlotGroup --
 *     A collection of plots that should be synchronized, e.g., by having the same X axis.
 */
class PlotGroup {

    friend class Plot;

public:
    PlotGroup();
    virtual ~PlotGroup();

    void view_back();
    void view_forward();
    void view_reset();
    void view_reset_x();
    void view_sync(Plot &source, bool in_place = false);

    /*
     * active_plot --
     *     Get the active plot, i.e., the last plot with which the user interacted.
     */
    inline Plot *
    active_plot()
    {
        return m_active_plot;
    }

protected:
    void add(Plot &plot);

    std::vector<Plot *> m_plots;
    Plot *m_active_plot;
};

#endif /* BLOCK_TRACE_EXPLORER_PLOT_H */
