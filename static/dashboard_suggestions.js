document.querySelectorAll('.suggestion-status-select').forEach((select) => {
  select.addEventListener('change', () => {
    const row = select.closest('.suggestion-entry');
    if (!row) return;

    const next = select.value;
    row.dataset.status = next;

    let deleteBtn = row.querySelector('.delete-suggestion-btn');
    if (next === 'Closed') {
      if (!deleteBtn) {
        deleteBtn = document.createElement('button');
        deleteBtn.type = 'button';
        deleteBtn.className = 'delete-suggestion-btn';
        deleteBtn.textContent = 'Delete';
        row.querySelector('.suggest-actions')?.appendChild(deleteBtn);
      }
    } else if (deleteBtn) {
      deleteBtn.remove();
    }
  });
});

document.addEventListener('click', (event) => {
  const deleteBtn = event.target.closest('.delete-suggestion-btn');
  if (!deleteBtn) return;
  deleteBtn.closest('.suggestion-entry')?.remove();
});
