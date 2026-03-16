(() => {
  const path = (location.pathname || "").toLowerCase();
  const file = path.split("/").pop() || "index.html";
  const allow = new Set(["", "index.html", "register.html"]);
  if (allow.has(file)) return;
  if (location.hostname.includes("monitor.nexo.hk")) return;

  async function checkAuth() {
    try {
      const res = await fetch("/api/auth/me", { credentials: "include", cache: "no-store" });
      const data = await res.json().catch(() => ({}));
      if (!res.ok || !data?.authenticated) {
        const next = encodeURIComponent(`${location.pathname}${location.search || ""}`);
        location.replace(`/register.html?next=${next}`);
      }
    } catch (_err) {
      const next = encodeURIComponent(`${location.pathname}${location.search || ""}`);
      location.replace(`/register.html?next=${next}`);
    }
  }

  checkAuth();
})();

