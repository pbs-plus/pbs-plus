Ext.define("PBS.window.D2DPathSelector", {
  extend: "Proxmox.window.FileBrowser",
  alias: "widget.pbsD2DPathSelector",

  title: gettext("Select Path"),

  controller: {
    xclass: "Ext.app.ViewController",
    init: function (view) {
      this.constructor.prototype.init.apply(this, arguments);
    },

    onSelect: function () {
      let me = this;
      let tree = me.lookup("tree");
      let selection = tree.getSelection();

      if (selection && selection.length > 0) {
        let data = selection[0].data;

        let decodedPath = atob(data.filepath);

        decodedPath = decodedPath.replace(/\/+/g, "/");
        if (!decodedPath.startsWith("/")) {
          decodedPath = "/" + decodedPath;
        }

        me.getView().fireEvent("select", decodedPath);
        me.getView().close();
      }
    },

    fileChanged: function () {
      let me = this;
      let tree = me.lookup("tree");
      let selection = tree.getSelection();
      me.lookup("selectBtn").setDisabled(!selection || selection.length < 1);
    },
  },

  fbar: [
    {
      xtype: "button",
      text: gettext("Select"),
      reference: "selectBtn",
      disabled: true,
      handler: "onSelect",
    },
  ],
});
