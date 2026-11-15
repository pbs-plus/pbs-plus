Ext.define("PBS.plugin.Responsive", {
  extend: "Ext.plugin.Abstract",
  alias: "plugin.pbsPlusResponsive",

  init: function (component) {
    this.prepare(component);
  },

  prepare: function (component) {
    let me = this;
    if (component.pbsPlusResponsiveReady) {
      return;
    }
    component.pbsPlusResponsiveReady = true;

    if (component.isXType("window")) {
      me.window(component);
    }
    if (component.isXType("toolbar")) {
      component.getLayout().setOverflowHandler("scroller");
    }
    if (
      component.isColumn &&
      component.flex &&
      !component.initialConfig.minWidth
    ) {
      component.minWidth = Math.min(
        100 * component.flex,
        component.maxWidth || Infinity,
      );
    }
    if (
      component.isXType("inputpanel") &&
      (component.column1 || component.column2)
    ) {
      component.items.each(function (row) {
        if (row.getLayout().type === "hbox") {
          me.columns(row);
        }
      });
    }
    if (component.pbsPlusResponsiveColumns) {
      me.columns(component);
    }
    if (component.isXType("combobox")) {
      let fitPicker = function (field) {
        let picker = field.getPicker();
        let width = Math.max(1, Ext.Element.getViewportWidth() - 16);
        let rect = field.el.dom.getBoundingClientRect();
        let height = Math.max(
          1,
          Math.max(rect.top, Ext.Element.getViewportHeight() - rect.bottom) - 8,
        );
        let limits = picker.pbsPlusViewportLimits;
        if (!limits) {
          limits = picker.pbsPlusViewportLimits = {
            minWidth: picker.minWidth || 0,
            maxWidth: picker.maxWidth || Infinity,
            minHeight: picker.minHeight || 0,
            maxHeight: picker.maxHeight || Infinity,
          };
        }
        picker.minWidth = Math.min(limits.minWidth, width);
        picker.maxWidth = Math.min(limits.maxWidth, width);
        picker.minHeight = Math.min(limits.minHeight, height);
        picker.maxHeight = Math.min(limits.maxHeight, height);
        let preferredWidth = field.matchFieldWidth
          ? field.getWidth()
          : picker.initialConfig.width;
        let preferredHeight = picker.initialConfig.height;
        picker.setSize(
          Math.min(
            typeof preferredWidth === "number"
              ? preferredWidth
              : picker.getWidth(),
            picker.maxWidth,
          ),
          Math.min(
            typeof preferredHeight === "number"
              ? preferredHeight
              : picker.getHeight(),
            picker.maxHeight,
          ),
        );
        field.alignPicker();
      };
      component.on("expand", fitPicker);
      component.on("resize", function (field) {
        if (field.isExpanded) {
          fitPicker(field);
        }
      });
    }

    if (component.isContainer) {
      component.on("add", function (owner, child) {
        me.prepare(child);
      });
      component.on("dockedadd", function (owner, child) {
        me.prepare(child);
      });
      component.getRefItems().forEach(function (child) {
        me.prepare(child);
      });
    }
  },

  window: function (win) {
    let preferredWidth = win.width;
    let preferredHeight = win.height;
    let limits = {
      minWidth: win.minWidth || 0,
      minHeight: win.minHeight || 0,
      maxWidth: win.maxWidth || Infinity,
      maxHeight: win.maxHeight || Infinity,
    };
    let fit = function () {
      if (win.destroyed || win.destroying) {
        return;
      }
      let width = Math.max(1, Ext.Element.getViewportWidth() - 16);
      let height = Math.max(1, Ext.Element.getViewportHeight() - 16);
      win.minWidth = Math.min(limits.minWidth, width);
      win.minHeight = Math.min(limits.minHeight, height);
      win.maxWidth = Math.min(limits.maxWidth, width);
      win.maxHeight = Math.min(limits.maxHeight, height);
      win.setSize(
        typeof preferredWidth === "number"
          ? Math.min(preferredWidth, win.maxWidth)
          : undefined,
        typeof preferredHeight === "number"
          ? Math.min(preferredHeight, win.maxHeight)
          : undefined,
      );
      if (win.rendered) {
        win.updateLayout();
        win.doConstrain();
      }
    };
    win.constrain = true;
    if (win.formPanel) {
      win.setLayout("fit");
      win.formPanel.setScrollable(true);
    } else {
      win.setScrollable(true);
      win.query("form").forEach(function (form) {
        form.setScrollable(true);
      });
    }
    fit();
    win.on("beforeshow", fit);
    win.on("afterrender", function () {
      if (win.resizer) {
        win.mon(win.resizer, "resize", function (resizer, width, height) {
          preferredWidth = width;
          preferredHeight = height;
        });
      }
    });
    win.mon(Ext.GlobalEvents, "resize", fit);
  },

  columns: function (row) {
    let columns = row.items.items;
    if (!columns.length) {
      return;
    }
    row.setLayout("column");
    columns.forEach(function (column) {
      column.flex = undefined;
      column.width = undefined;
      column.padding = 0;
    });
    let narrow;
    let reflow = function () {
      let width = row.rendered
        ? row.getWidth()
        : Ext.Element.getViewportWidth();
      let next = width < 600;
      if (next === narrow) {
        return;
      }
      narrow = next;
      columns.forEach(function (column, index) {
        column.columnWidth = narrow ? 1 : 1 / columns.length;
        column.setMargin(narrow ? "0 0 8 0" : index ? "0 0 0 10" : "0 10 0 0");
      });
      row.updateLayout();
    };
    row.on("resize", reflow);
    reflow();
  },
});

Ext.define("PBS.plusWindow.Window", {
  extend: "Ext.window.Window",
  plugins: ["pbsPlusResponsive"],
});
