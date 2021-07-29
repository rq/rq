document.addEventListener('DOMContentLoaded', () => {
    let checkbox = document.querySelector('#darkmode');
    checkbox.checked = localStorage.getItem('theme') === null ?
        window.matchMedia("(prefers-color-scheme: dark)").matches : (localStorage.getItem('theme') === 'dark');

    document.body.className = checkbox.checked ? 'dark' : 'light';

    function syncDarkmode() {
        let theme = checkbox.checked ? 'dark' : 'light';
        localStorage.setItem('theme', theme);
        document.body.className = theme;
    }
    checkbox.addEventListener('input', syncDarkmode);
});
