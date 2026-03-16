(() => {
  const path = (location.pathname || "").toLowerCase();
  const file = path.split("/").pop() || "index.html";
  const allow = new Set(["", "index.html", "register.html"]);
  if (allow.has(file)) return;
  if (location.hostname.includes("monitor.nexo.hk")) return;

  function isZh() {
    try {
      return localStorage.getItem("macro-monitor-lang") === "zh";
    } catch {
      return false;
    }
  }

  function mountAuthSession(user) {
    const header = document.querySelector(".site-header");
    if (!header) return;
    const email = String(user?.email || "").trim();
    if (!email) return;

    let actions = document.getElementById("site-header-actions");
    const langBtn = document.getElementById("lang-toggle") || document.getElementById("stock-lang-toggle");
    if (!actions) {
      actions = document.createElement("div");
      actions.id = "site-header-actions";
      actions.style.display = "inline-flex";
      actions.style.alignItems = "center";
      actions.style.justifyContent = "flex-end";
      actions.style.gap = "0.45rem";
      actions.style.flexWrap = "wrap";
      actions.style.minWidth = "0";
      if (langBtn && langBtn.parentElement === header) {
        header.insertBefore(actions, langBtn);
        actions.appendChild(langBtn);
      } else {
        header.appendChild(actions);
      }
    }

    let emailNode = document.getElementById("public-auth-email");
    if (!emailNode) {
      emailNode = document.createElement("span");
      emailNode.id = "public-auth-email";
      emailNode.style.fontSize = "0.86rem";
      emailNode.style.color = "rgba(18,36,40,0.78)";
      actions.appendChild(emailNode);
    }

    let logoutBtn = document.getElementById("public-auth-logout");
    if (!logoutBtn) {
      logoutBtn = document.createElement("button");
      logoutBtn.id = "public-auth-logout";
      logoutBtn.type = "button";
      logoutBtn.className = "btn ghost";
      logoutBtn.style.padding = "0.44rem 0.7rem";
      logoutBtn.style.minWidth = "72px";
      logoutBtn.addEventListener("click", async () => {
        logoutBtn.disabled = true;
        try {
          await fetch("/api/auth/logout", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            credentials: "include",
          });
        } catch {
          // Ignore and force clear in browser flow via redirect.
        }
        location.replace("/register.html");
      });
      actions.appendChild(logoutBtn);
    }

    const zh = isZh();
    emailNode.textContent = zh ? `已登录：${email}` : `Signed in: ${email}`;
    logoutBtn.textContent = zh ? "退出" : "Sign out";

    if (langBtn && !langBtn.dataset.authLangBound) {
      langBtn.dataset.authLangBound = "1";
      langBtn.addEventListener("click", () => {
        const zhNext = isZh();
        emailNode.textContent = zhNext ? `已登录：${email}` : `Signed in: ${email}`;
        logoutBtn.textContent = zhNext ? "退出" : "Sign out";
      });
    }
  }

  async function checkAuth() {
    try {
      const res = await fetch("/api/auth/me", { credentials: "include", cache: "no-store" });
      const data = await res.json().catch(() => ({}));
      if (!res.ok || !data?.authenticated) {
        const next = encodeURIComponent(`${location.pathname}${location.search || ""}`);
        location.replace(`/register.html?next=${next}`);
        return;
      }
      mountAuthSession(data?.user || {});
    } catch (_err) {
      const next = encodeURIComponent(`${location.pathname}${location.search || ""}`);
      location.replace(`/register.html?next=${next}`);
    }
  }

  checkAuth();
})();
