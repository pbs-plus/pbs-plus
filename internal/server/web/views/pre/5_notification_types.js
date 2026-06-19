// Patch PBS notification matcher UI to include D2D notification types.
//
// PBS registers notification type descriptions via
//   Proxmox.Utils.overrideNotificationFieldValue({ key: 'Description' })
// in www/Utils.js. The matcher rule editor's "type" dropdown grid uses
// formatNotificationFieldValue() which does a lookup in that table,
// falling back to the raw value string if no entry exists.
//
// We do two things:
//   1. Register our D2D type descriptions in the lookup table so the
//      Comment column shows human-readable text.
//   2. Inject the D2D types into the matcher-field-values API response
//      so they appear as selectable rows in the dropdown grid.
//
// Severity levels used by D2D notifications (matching PBS upstream):
//   info     -  Job completed successfully with no issues
//   notice   -  Job succeeded with warnings (e.g. partial file verification)
//   warning  -  System alert (stale backup, unconfigured target, target offline)
//   error    -  Job failed entirely
(function () {
  // Register human-readable descriptions for the Comment column.
  // These appear in Datacenter → Notifications → Matchers → type dropdown.
  Proxmox.Utils.overrideNotificationFieldValue({
    "d2d-backup": "D2D Backup  -  disk-to-disk backup job completed",
    "d2d-restore": "D2D Restore  -  file/folder restore job completed",
    "d2d-verification": "D2D Verification  -  spot-check verification run completed",
    "d2d-batch": "D2D Batch  -  consolidated notification for a job batch",
    "d2d-alert-stale-backup": "D2D Alert: backup job has not run within threshold",
    "d2d-alert-unconfigured-target": "D2D Alert: target has no backup job configured",
    "d2d-alert-target-offline": "D2D Alert: configured target is unreachable",
  });

  // Inject D2D type entries into the matcher-field-values API response
  // so they appear as selectable rows alongside PBS built-in types.
  var D2D_NOTIFY_TYPES = [
    { field: "type", value: "d2d-backup", comment: "D2D Backup" },
    { field: "type", value: "d2d-restore", comment: "D2D Restore" },
    { field: "type", value: "d2d-verification", comment: "D2D Verification" },
    { field: "type", value: "d2d-batch", comment: "D2D Batch" },
    { field: "type", value: "d2d-alert-stale-backup", comment: "D2D Alert: Stale Backup" },
    {
      field: "type",
      value: "d2d-alert-unconfigured-target",
      comment: "D2D Alert: Unconfigured Target",
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
