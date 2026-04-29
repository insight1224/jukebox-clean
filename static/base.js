function toggleMenu() {
  const menu = document.getElementById("navLinks");
  const toggle = document.querySelector(".menu-toggle");
  if (!menu || !toggle) return;

  const isActive = menu.classList.toggle("active");
  toggle.setAttribute("aria-expanded", String(isActive));
}

document.addEventListener("click", (event) => {
  const menu = document.getElementById("navLinks");
  const toggle = document.querySelector(".menu-toggle");
  if (!menu || !toggle) return;
  if (!menu.classList.contains("active")) return;

  const clickedToggle = toggle.contains(event.target);
  const clickedMenu = menu.contains(event.target);
  if (!clickedToggle && !clickedMenu) {
    menu.classList.remove("active");
    toggle.setAttribute("aria-expanded", "false");
  }
});
