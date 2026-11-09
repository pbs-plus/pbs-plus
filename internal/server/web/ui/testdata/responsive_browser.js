(async function () {
  const nextFrame = () =>
    new Promise((resolve) =>
      requestAnimationFrame(() => requestAnimationFrame(resolve)),
    );
  const results = [];
  const names = Object.keys(Ext.ClassManager.classes).filter(
    (name) =>
      /^(PBS\.D2DManagement\.|PBS\.D2DVerification\.|PBS\.MtfManagement\.)/.test(
        name,
      ) && Ext.ClassManager.classes[name].prototype?.isWindow,
  );
  const check = async (name, create) => {
    let win;
    try {
      win = create();
      await nextFrame();
      const rect = win.el.dom.getBoundingClientRect();
      const inside =
        rect.left >= 0 &&
        rect.top >= 0 &&
        rect.right <= innerWidth + 1 &&
        rect.bottom <= innerHeight + 1;
      const footers = win
        .query("toolbar[dock=bottom]")
        .map((bar) => bar.el.dom.getBoundingClientRect());
      const footerInside = footers.every(
        (bar) => bar.top >= 0 && bar.bottom <= innerHeight + 1,
      );
      results.push({
        name,
        inside,
        footerInside,
        width: rect.width,
        height: rect.height,
      });
    } catch (error) {
      results.push({ name, error: String(error) });
    } finally {
      win?.destroy();
    }
  };
  for (const name of names) {
    await check(name, () =>
      Ext.create(name, { isCreate: true, batchName: "" }).show(),
    );
  }
  await check("Bulk edit", () =>
    Ext.create("PBS.window.D2DBulkJobEdit", {
      jobIds: ["responsive-test"],
      fields: Array.from({ length: 12 }, (_, i) => ({
        name: "setting" + i,
        label: "Setting " + i,
        field: { xtype: "textfield" },
      })),
    }).show(),
  );
  const snapshotField = Ext.create("PBS.form.D2DSnapshotSelector");
  await check("Snapshot browser", () =>
    Ext.create("PBS.window.D2DSnapshotBrowser", { snapshotField }).show(),
  );
  snapshotField.destroy();
  const fixture = window.pbsPlusResponsiveFixture;
  if (fixture) {
    await check("Compose snapshot", () =>
      Ext.create("PBS.D2DSnapshotMount.ComposeWindow", fixture).show(),
    );
    await check("Path browser", () =>
      Ext.create("PBS.window.D2DPathSelector", {
        listURL: "/api2/json/admin/datastore/" + fixture.datastore + "/catalog",
        extraParams: {
          "backup-id": fixture.backupId,
          "backup-type": fixture.backupType,
          "backup-time": Date.parse(fixture.backupTime) / 1000,
        },
      }).show(),
    );
  }
  for (const xtype of [
    "pbsPlusMountProfilesPanel",
    "pbsPlusOutpostsPanel",
    "pbsPlusActiveMountsPanel",
  ]) {
    const name = Ext.ClassManager.getNameByAlias("widget." + xtype);
    if (!name) {
      results.push({ name: xtype, error: "Unknown component alias" });
      continue;
    }
    let panel;
    await check(xtype, () => {
      panel = Ext.create(name);
      const controller = panel.getController();
      if (xtype === "pbsPlusMountProfilesPanel") controller.openEdit(panel);
      else if (xtype === "pbsPlusOutpostsPanel") controller.openEdit(null);
      else controller.initNew(panel);
      return Ext.ComponentQuery.query("window")
        .filter((win) => win.isVisible())
        .at(-1);
    });
    panel?.destroy();
  }
  window.pbsPlusResponsiveResults = results;
  return JSON.stringify({
    viewport: [innerWidth, innerHeight],
    checked: results.length,
    failures: results.filter(
      (result) => !result.inside || !result.footerInside || result.error,
    ),
  });
})();
