/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "main_window.h"
#include "plot.h"
#include "trace.h"

/*
 * MainWindow::MainWindow --
 *     Create the main window object.
 */
MainWindow::MainWindow(TraceCollection &traces)
    : m_toolbar_box(Gtk::Orientation::HORIZONTAL), m_back_button("<"), m_forward_button(">"),
      m_label1("     "), m_inspect_toggle("_Inspect", true), m_move_toggle("_Move", true),
      m_zoom_toggle("_Zoom", true), m_label2("     "), m_zoom_in_button("+"),
      m_zoom_out_button("-"), m_reset_view_button("_Reset", true),
      m_main_box(Gtk::Orientation::VERTICAL)
{
    set_title("I/O Trace Explorer");

    /* Create the plots */

    for (auto iter : traces.traces()) {
        Plot *p = new Plot(m_plot_group, *(iter.second));
        p->set_expand(true);
        m_plots.push_back(p);
    }
    m_plot_group.view_reset_x();

    /* The toolbar */

    m_toolbar_box.append(m_back_button);
    m_toolbar_box.append(m_forward_button);
    m_toolbar_box.append(m_label1);
    m_toolbar_box.append(m_inspect_toggle);
    m_toolbar_box.append(m_move_toggle);
    m_toolbar_box.append(m_zoom_toggle);
    m_toolbar_box.append(m_label2);
    m_toolbar_box.append(m_zoom_in_button);
    m_toolbar_box.append(m_zoom_out_button);
    m_toolbar_box.append(m_reset_view_button);

    m_inspect_toggle_connection = m_inspect_toggle.signal_toggled().connect(
      sigc::mem_fun(*this, &MainWindow::on_inspect_toggle));
    m_move_toggle_connection =
      m_move_toggle.signal_toggled().connect(sigc::mem_fun(*this, &MainWindow::on_move_toggle));
    m_zoom_toggle_connection =
      m_zoom_toggle.signal_toggled().connect(sigc::mem_fun(*this, &MainWindow::on_zoom_toggle));

    m_back_button.signal_clicked().connect(sigc::mem_fun(*this, &MainWindow::on_view_back));
    m_forward_button.signal_clicked().connect(sigc::mem_fun(*this, &MainWindow::on_view_forward));
    m_zoom_in_button.signal_clicked().connect(sigc::mem_fun(*this, &MainWindow::on_zoom_in));
    m_zoom_out_button.signal_clicked().connect(sigc::mem_fun(*this, &MainWindow::on_zoom_out));
    m_reset_view_button.signal_clicked().connect(sigc::mem_fun(*this, &MainWindow::on_view_reset));

    set_plot_tool(PlotTool::INSPECT);

    /* The main view */

    m_main_box.set_expand();
    m_main_box.append(m_toolbar_box);

    Gtk::Paned *first_paned = NULL;
    Gtk::Paned *last_paned = NULL;
    Plot *last_plot = NULL;

    for (Plot *p : m_plots) {
        if (last_paned == NULL) {
            if (last_plot == NULL) {
                /* This is the first plot: There is nothing to do. */
            } else {
                /* This is the second plot: Create a new pane. */
                Gtk::Paned *paned = new Gtk::Paned(Gtk::Orientation::VERTICAL);
                paned->set_wide_handle(true);
                paned->set_expand(true);
                paned->set_start_child(*last_plot);
                m_panes.push_back(paned);
                first_paned = last_paned = paned;
            }
        } else {
            /* Subsequent plots. */
            Gtk::Paned *paned = new Gtk::Paned(Gtk::Orientation::VERTICAL);
            paned->set_wide_handle(true);
            paned->set_expand(true);
            paned->set_start_child(*last_plot);
            m_panes.push_back(paned);
            last_paned->set_end_child(*paned);
            last_paned = paned;
        }
        last_plot = p;
    }

    if (last_paned == NULL) {
        if (last_plot != NULL) {
            m_main_box.append(*last_plot);
        }
    } else {
        last_paned->set_end_child(*last_plot);
        m_main_box.append(*first_paned);
    }

    /* Set up events */

    auto keyController = Gtk::EventControllerKey::create();
    keyController->signal_key_pressed().connect(
      sigc::mem_fun(*this, &MainWindow::on_window_key_pressed), false);
    add_controller(keyController);

    /* Finish setting up the window */

    m_main_box.append(m_status_bar);
    set_child(m_main_box);
    set_resizable(true);
    set_default_size(1024, 768);
}

/*
 * MainWindow::~MainWindow --
 *     Delete the main window.
 */
MainWindow::~MainWindow()
{
    /*
     * TODO: Do we need to clean up plots and panes? It seems that the destructor is not called on
     * application exit.
     */
}

/*
 * MainWindow::set_plot_tool --
 *     Set the tool for user interaction.
 */
void
MainWindow::set_plot_tool(PlotTool tool)
{
    for (Plot *p : m_plots)
        p->set_active_tool(tool);

    m_inspect_toggle_connection.block();
    m_move_toggle_connection.block();
    m_zoom_toggle_connection.block();

    m_inspect_toggle.set_active(tool == PlotTool::INSPECT);
    m_move_toggle.set_active(tool == PlotTool::MOVE);
    m_zoom_toggle.set_active(tool == PlotTool::ZOOM);

    m_inspect_toggle_connection.unblock();
    m_move_toggle_connection.unblock();
    m_zoom_toggle_connection.unblock();
}

/*
 * MainWindow::on_inspect_toggle --
 *     Event handler for the "inspect" tool toggle button.
 */
void
MainWindow::on_inspect_toggle()
{
    set_plot_tool(PlotTool::INSPECT);
}

/*
 * MainWindow::on_move_toggle --
 *     Event handler for the "move" tool toggle button.
 */
void
MainWindow::on_move_toggle()
{
    set_plot_tool(PlotTool::MOVE);
}

/*
 * MainWindow::on_zoom_toggle --
 *     Event handler for the "zoom" tool toggle button.
 */
void
MainWindow::on_zoom_toggle()
{
    set_plot_tool(PlotTool::ZOOM);
}

/*
 * MainWindow::on_view_back --
 *     Event handler for the view back button.
 */
void
MainWindow::on_view_back()
{
    m_plot_group.view_back();
}

/*
 * MainWindow::on_view_forward --
 *     Event handler for the view forward button.
 */
void
MainWindow::on_view_forward()
{
    m_plot_group.view_forward();
}

/*
 * MainWindow::on_zoom_in --
 *     Event handler for the zoom in button.
 */
void
MainWindow::on_zoom_in()
{
    if (m_plot_group.active_plot())
        m_plot_group.active_plot()->zoom_in();
}

/*
 * MainWindow::on_zoom_out --
 *     Event handler for the zoom out button.
 */
void
MainWindow::on_zoom_out()
{
    if (m_plot_group.active_plot())
        m_plot_group.active_plot()->zoom_out();
}

/*
 * MainWindow::on_view_reset --
 *     Event handler for the reset view button.
 */
void
MainWindow::on_view_reset()
{
    m_plot_group.view_reset();
}

/*
 * MainWindow::on_window_key_pressed --
 *     Event handler for key press events.
 */
bool
MainWindow::on_window_key_pressed(guint key, guint, Gdk::ModifierType state)
{
    if (key == GDK_KEY_BackSpace ||
      (key == GDK_KEY_z &&
        (state & Gdk::ModifierType::META_MASK) == Gdk::ModifierType::META_MASK) ||
      (key == GDK_KEY_z &&
        (state & Gdk::ModifierType::CONTROL_MASK) == Gdk::ModifierType::CONTROL_MASK)) {
        on_view_back();
        return true;
    }

    if ((key == GDK_KEY_Z &&
          (state & Gdk::ModifierType::META_MASK) == Gdk::ModifierType::META_MASK) ||
      (key == GDK_KEY_Z &&
        (state & Gdk::ModifierType::CONTROL_MASK) == Gdk::ModifierType::CONTROL_MASK)) {
        on_view_forward();
        return true;
    }

    if (key == GDK_KEY_minus || key == GDK_KEY_underscore) {
        on_zoom_out();
        return true;
    }

    if (key == GDK_KEY_plus || key == GDK_KEY_equal) {
        on_zoom_in();
        return true;
    }

    if (key == GDK_KEY_i || key == GDK_KEY_I) {
        on_inspect_toggle();
        return true;
    }

    if (key == GDK_KEY_m || key == GDK_KEY_M) {
        on_move_toggle();
        return true;
    }

    if (key == GDK_KEY_z || key == GDK_KEY_Z) {
        on_zoom_toggle();
        return true;
    }

    if (key == GDK_KEY_r || key == GDK_KEY_R) {
        on_view_reset();
        return true;
    }

    return false;
}
