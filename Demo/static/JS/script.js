document.addEventListener("DOMContentLoaded", function () {
    const dropdownButtons = document.querySelectorAll(".dropbtn");
    dropdownButtons.forEach(button => {
        button.addEventListener("click", function (e) {
        // Toggle this one
            const dropdownContent = button.nextElementSibling;
            dropdownContent.classList.toggle("show");
        });
    });

    // Close dropdown if clicking outside
    window.addEventListener("click", function (e) {
        if (!e.target.matches('.dropbtn')) {
            document.querySelectorAll(".dropdown-content").forEach(content => {
                content.classList.remove("show");
            });
        }
    });

    document.querySelectorAll('.dropdown').forEach(dropdown => {
    const button = dropdown.querySelector('.dropbtn');
    const links = dropdown.querySelectorAll('.dropdown-content a');
    const dropdownButtons = document.querySelectorAll(".dropbtn");
    const currentPath = window.location.pathname;

    let foundActive = false;

    links.forEach(link => {
      const linkPath = new URL(link.href).pathname;

      if (linkPath === currentPath) {
        link.classList.add('active');
        const dropdownContent = button.nextElementSibling;
        dropdownContent.classList.add("show");
        foundActive = true;
      } else {
        link.classList.remove('active');
      }
    });

    // Toggle "active" class on the button if any link is active
    if (foundActive) {
      button.classList.add('active');
    } else {
      button.classList.remove('active');
    }
  });
});


$(document).ready(function () {
  // $('.table-class').DataTable({
  //     paging: false,
  //     ordering: false,
  //     info: false,
  //     retrieve: true,
  //     destroy: true,
  //     autoWidth: false
  // });

  // To allow search in the drop-downs
  $('select.form-select').select2({
    theme: 'bootstrap-5',
    placeholder: 'اختر',
    allowClear: true
  });

  let initialSort = [[0, 'desc']]; // Default

  if ($('table').hasClass('default-order-1')) {
    initialSort = [[1, 'desc']];
  }

  let ordering = true;

  if ($('table').hasClass('no-order')) {
    ordering = false;
  }


  const table_big = $('.data-table-big').DataTable({
    paging: true,
    ordering: ordering,
    order: initialSort,
    searching: true,
    info: false,
    lengthChange: false,
    deferRender: true,
    pageLength: 200,
    dom: 't',  // ← IMPORTANT: only table, no UI controls
  });

  $('#table-search-big').on('keyup', function () {
    table_big.search(this.value).draw();
  });

  function syncPagination() {
    const info = table_big.page.info();
    const currentPage = info.page + 1;
    const totalPages = info.pages;

    // Update the "Page X of Y" text
    $('#page-status').text(`Page ${currentPage} of ${totalPages}`);

    // Update Button States
    $('#prev-item').toggleClass('disabled', info.page === 0);
    $('#next-item').toggleClass('disabled', info.page >= totalPages - 1 || totalPages === 0);
  }

  table_big.on('load', function () {
    syncPagination();
  });

  // Trigger sync on every draw (Pagination, Search, Sort)
  table_big.on('draw', function () {
    syncPagination();
  });

  // Bootstrap Button Click Events
  $('#next-btn').on('click', function (e) {
    e.preventDefault();
    table_big.page('next').draw('page');
  });

  $('#prev-btn').on('click', function (e) {
    e.preventDefault();
    table_big.page('previous').draw('page');
  });

});