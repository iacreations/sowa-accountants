// static/sowaf/js/add_new_select.js
(function () {
  document.addEventListener("change", function (e) {
    const sel = e.target;

    // Only handle <select> elements that declare a data-add-url
    if (!sel || sel.tagName !== "SELECT") return;
    const addUrl = sel.getAttribute("data-add-url");
    if (!addUrl) return;

    // Trigger when the option value is add_new
    if (sel.value === "add_new") {
      window.open(addUrl, "_blank", "noopener");
      sel.value = ""; // reset back to blank
      // Do NOT dispatch change again (avoids messing with your existing logic)
    }
  });
})();
