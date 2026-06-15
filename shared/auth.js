// SWEAT440 Dashboard — Authentication Utilities
// Requires users.js to be loaded before this file.

const AUTH_SESSION_KEY = 'sw440_auth';

// Returns the authenticated user object from sessionStorage.
// Redirects to login.html if no valid session exists.
function checkAuth() {
  const raw = sessionStorage.getItem(AUTH_SESSION_KEY);
  if (!raw) {
    _redirectToLogin();
    return null;
  }
  try {
    return JSON.parse(raw);
  } catch (e) {
    sessionStorage.removeItem(AUTH_SESSION_KEY);
    _redirectToLogin();
    return null;
  }
}

// Returns the array of allowed studio names, or null if the user has full access.
function getAuthStudios(user) {
  if (!user || user.role === 'admin' || user.studios === null) return null;
  return user.studios;
}

// Clears the session and redirects to the login page.
function logout() {
  sessionStorage.removeItem(AUTH_SESSION_KEY);
  _redirectToLogin();
}

function _redirectToLogin() {
  const isNso = location.pathname.includes('/nso-dashboard/');
  location.replace(isNso ? '../login.html' : 'login.html');
}

// Renders the user info + sign-out button into the nav.
// Call once after checkAuth() succeeds.
function renderAuthNav(user) {
  const nav = document.querySelector('.nav');
  if (!nav || !user) return;
  const el = document.createElement('div');
  el.className = 'nav-user';
  el.innerHTML = `
    <span class="nav-user-name">${user.name}</span>
    <button class="logout-btn" onclick="logout()">Sign out</button>
  `;
  nav.appendChild(el);
}
