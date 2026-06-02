// Patch PBS notification matcher value store to include D2D notification types.
//
// PBS hardcodes the "type" dropdown values in Rust (acme, gc, sync, verify, etc.)
// in proxmox-notify's matcher-field-values API. We intercept the Ajax response
// and append our custom D2D types so they appear in the matcher rule editor's
// "type" dropdown under Datacenter → Notifications → Matchers.
//
// Severity levels used by D2D notifications (matching PBS upstream):
//   info    — Job completed successfully with no issues
//   notice  — Job succeeded with warnings (e.g. partial file verification)
//   warning — System alert (stale backup, unconfigured target, target offline)
//   error   — Job failed entirely
//
// These map to the same severity enum used by PBS's built-in notifications
// (gc, sync, verify, etc.) and can be filtered in matcher rules.
(function () {
  var D2D_NOTIFY_TYPES = [
    {
      field: "type",
      value: "d2d-backup",
      comment: "D2D Backup — triggered when a disk-to-disk backup job completes (info on success, error on failure)",
    },
    {
      field: "type",
      value: "d2d-restore",
      comment: "D2D Restore — triggered when a file/folder restore job completes (info on success, error on failure)",
    },
    {
      field: "type",
      value: "d2d-verification",
      comment: "D2D Verification — triggered when a spot-check verification run completes (info if all files match, notice on warnings, error on failure)",
    },
    {
      field: "type",
      value: "d2d-batch",
      comment: "D2D Batch — consolidated notification sent when all jobs in a notification batch complete (info if all succeeded, error if any failed)",
    },
    {
      field: "type",
      value: "d2d-alert-stale-backup",
      comment: "D2D Alert: Stale Backup — warning when a backup job has not completed within the configured threshold (default 7 days)",
    },
    {
      field: "type",
      value: "d2d-alert-unconfigured-target",
      comment: "D2D Alert: Unconfigured Target — warning when a target exists but has no backup job assigned to it",
    },
    {
      field: "type",
      value: "d2d-alert-target-offline",
      comment: "D2D Alert: Target Offline — warning when a configured target cannot be reached",
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
