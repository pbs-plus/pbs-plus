// Patch PBS notification matcher value store to include D2D notification types.
// PBS hardcodes the "type" dropdown values in Rust (acme, gc, sync, verify, etc.)
// We intercept the Ajax response and append our custom D2D types.
(function () {
  const D2D_NOTIFY_TYPES = [
    { field: "type", value: "d2d-backup", comment: "D2D Disk Backup" },
    { field: "type", value: "d2d-restore", comment: "D2D Disk Restore" },
    { field: "type", value: "d2d-verification", comment: "D2D Verification" },
    { field: "type", value: "d2d-batch", comment: "D2D Batch Notification" },
    {
      field: "type",
      value: "d2d-alert-unconfigured-target",
      comment: "D2D Alert: Unconfigured Target",
    },
    {
      field: "type",
      value: "d2d-alert-stale-backup",
      comment: "D2D Alert: Stale Backup",
    },
    {
      field: "type",
      value: "d2d-alert-target-offline",
      comment: "D2D Alert: Target Offline",
    },
  ];

  Ext.Ajax.on(
    "requestcomplete",
    function (conn, response, options) {
      try {
        var url = options.url || "";
        if (url.indexOf("matcher-field-values") === -1) return;

        var data = Ext.decode(response.responseText);
        if (!data || !Ext.isArray(data.data)) return;

        // Collect existing type values to avoid duplicates
        var existing = {};
        Ext.Array.each(data.data, function (rec) {
          if (rec.field === "type") existing[rec.value] = true;
        });

        // Append any D2D types not already present
        Ext.Array.each(D2D_NOTIFY_TYPES, function (t) {
          if (!existing[t.value]) {
            data.data.push(t);
          }
        });

        // Re-encode so the store reader picks up the extra records
        response.responseText = Ext.encode(data);
      } catch (_e) {
        // Don't break the UI if something goes wrong
      }
    },
    null,
    { priority: 999 }
  );
})();
