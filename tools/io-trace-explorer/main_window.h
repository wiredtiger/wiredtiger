/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef BLOCK_TRACE_EXPLORER_MAIN_WINDOW_H
#define BLOCK_TRACE_EXPLORER_MAIN_WINDOW_H

#include <gtkmm.h>

#include "plot.h"
#include "trace.h"

/*
 * MainWindow --
 *     The main window.
 */
class MainWindow : public Gtk::ApplicationWindow {

public:
    MainWindow(TraceCollection &traces);
    ~MainWindow();

protected:
    /* The toolbar (in the order of appearance). */
    Gtk::Box m_toolbar_box;
    Gtk::Button m_back_button;
    Gtk::Button m_forward_button;
    Gtk::Label m_label1;
    Gtk::ToggleButton m_inspect_toggle;
    Gtk::ToggleButton m_move_toggle;
    Gtk::ToggleButton m_zoom_toggle;
    Gtk::Label m_label2;
    Gtk::Button m_zoom_in_button;
    Gtk::Button m_zoom_out_button;
    Gtk::Button m_reset_view_button;

    /* Event connections for the tool toggle buttons. */
    sigc::connection m_inspect_toggle_connection;
    sigc::connection m_move_toggle_connection;
    sigc::connection m_zoom_toggle_connection;

    /* The main area. */
    Gtk::Box m_main_box;
    PlotGroup m_plot_group;
    std::vector<Plot *> m_plots;
    std::vector<Gtk::Paned *> m_panes;

    /* The status bar. */
    Gtk::Statusbar m_status_bar;

    /* Set the tool for user interaction with the plots. */
    void set_plot_tool(PlotTool tool);

    /* Event handlers. */
    void on_inspect_toggle();
    void on_move_toggle();
    void on_zoom_toggle();
    void on_view_back();
    void on_view_forward();
    void on_view_reset();
    void on_zoom_in();
    void on_zoom_out();
    bool on_window_key_pressed(guint key, guint, Gdk::ModifierType state);
};

#endif /* BLOCK_TRACE_EXPLORER_MAIN_WINDOW_H */