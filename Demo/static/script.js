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

    });