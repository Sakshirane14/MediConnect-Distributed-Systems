const API_BASE = window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1"
  ? "http://localhost:8000"
  : "";

function getToken() {
  return sessionStorage.getItem("mediconnect_token");
}

function getUser() {
  const raw = sessionStorage.getItem("mediconnect_user");
  return raw ? JSON.parse(raw) : null;
}

function setAuth(token, user) {
  sessionStorage.setItem("mediconnect_token", token);
  sessionStorage.setItem("mediconnect_user", JSON.stringify(user));
}

function clearAuth() {
  sessionStorage.removeItem("mediconnect_token");
  sessionStorage.removeItem("mediconnect_user");
}

async function apiFetch(path, options = {}) {
  const token = getToken();
  const headers = { "Content-Type": "application/json", ...options.headers };
  if (token) headers["Authorization"] = "Bearer " + token;
  const resp = await fetch(API_BASE + path, { ...options, headers });
  if (!resp.ok) {
    const err = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(err.detail || "Request failed");
  }
  return resp.json();
}

async function apiUpload(path, formData) {
  const token = getToken();
  const headers = {};
  if (token) headers["Authorization"] = "Bearer " + token;
  const resp = await fetch(API_BASE + path, { method: "POST", headers, body: formData });
  if (!resp.ok) {
    const err = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(err.detail || "Upload failed");
  }
  return resp.json();
}

function requireAuth() {
  if (!getToken()) {
    window.location.href = "/index.html";
    return false;
  }
  return true;
}

function logout() {
  clearAuth();
  window.location.href = "/index.html";
}
