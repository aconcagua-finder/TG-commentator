(() => {
  function findRow(el) {
    return el ? el.closest("[data-msg-row]") : null;
  }

  function textValue(textEl) {
    if (!textEl) return "";
    return (textEl.textContent || "").trim();
  }

  function startMessageEdit(btn) {
    const row = findRow(btn);
    if (!row) return;

    const textEl = row.querySelector("[data-msg-text]");
    const actionsEl = row.querySelector("[data-msg-actions]");
    const formEl = row.querySelector("[data-msg-edit-form]");
    if (!formEl) return;

    const textarea = formEl.querySelector("textarea");
    if (textarea) {
      // Prefer the server-provided original textarea value (may include role/mood prefix).
      // Fall back to the visible text element only if the textarea came in empty.
      if (!textarea.value) {
        textarea.value = textValue(textEl);
      }
      textarea.focus();
      textarea.setSelectionRange(textarea.value.length, textarea.value.length);
    }

    if (textEl) textEl.style.display = "none";
    if (actionsEl) actionsEl.style.display = "none";
    formEl.style.display = "";
  }

  function cancelMessageEdit(btn) {
    const row = findRow(btn);
    if (!row) return;

    const textEl = row.querySelector("[data-msg-text]");
    const actionsEl = row.querySelector("[data-msg-actions]");
    const formEl = row.querySelector("[data-msg-edit-form]");
    if (!formEl) return;

    if (textEl) textEl.style.display = "";
    if (actionsEl) actionsEl.style.display = "";
    formEl.style.display = "none";
  }

  window.startMessageEdit = startMessageEdit;
  window.cancelMessageEdit = cancelMessageEdit;
})();
