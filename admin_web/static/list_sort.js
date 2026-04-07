// Persist list sort selection across visits.
//
// Each list page renders a <form data-sort-form data-sort-list-type="..."> with a
// <select data-sort-select name="sort"> via the ui.sort_dropdown() macro.
// On submit we stash the chosen value under "list_sort:<list_type>".
// On page load, if the URL has no ?sort= param, we restore the stored value
// by setting it on the select and reloading once with the param appended.
// The double-load is unavoidable: server-side sorting needs the param to
// build the response, and we don't want to ship a giant JSON sort path just
// for this preference.

(() => {
  const STORAGE_PREFIX = "list_sort:";

  function storeKey(listType) {
    return STORAGE_PREFIX + listType;
  }

  function readStored(listType) {
    try {
      return window.localStorage.getItem(storeKey(listType));
    } catch (err) {
      return null;
    }
  }

  function writeStored(listType, value) {
    try {
      if (value) {
        window.localStorage.setItem(storeKey(listType), value);
      } else {
        window.localStorage.removeItem(storeKey(listType));
      }
    } catch (err) {
      // Quota / private mode — ignore.
    }
  }

  function urlHasSort(url) {
    try {
      return new URL(url, window.location.origin).searchParams.has("sort");
    } catch (err) {
      return false;
    }
  }

  function applyStoredSort(form) {
    const listType = form.getAttribute("data-sort-list-type");
    if (!listType) return;
    const select = form.querySelector("[data-sort-select]");
    if (!select) return;

    const stored = readStored(listType);
    if (!stored) return;

    // If URL already specifies a sort (user just changed it), trust the URL
    // and update the stored value to match.
    if (urlHasSort(window.location.href)) {
      if (select.value && select.value !== stored) {
        writeStored(listType, select.value);
      }
      return;
    }

    // Stored value isn't a real option — drop it and bail.
    const optionExists = Array.from(select.options).some((opt) => opt.value === stored);
    if (!optionExists) {
      writeStored(listType, null);
      return;
    }

    // Already showing the same default the server picked — nothing to do.
    if (select.value === stored) return;

    // Restore the stored value and reload once with the new query.
    const url = new URL(window.location.href);
    url.searchParams.set("sort", stored);
    window.location.replace(url.toString());
  }

  function watchFormSubmit(form) {
    form.addEventListener("submit", () => {
      const listType = form.getAttribute("data-sort-list-type");
      const select = form.querySelector("[data-sort-select]");
      if (!listType || !select) return;
      writeStored(listType, select.value || null);
    });
  }

  function init() {
    const forms = document.querySelectorAll("[data-sort-form]");
    forms.forEach((form) => {
      applyStoredSort(form);
      watchFormSubmit(form);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
