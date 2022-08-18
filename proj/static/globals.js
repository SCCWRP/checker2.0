const critical_error_handler = (contact) => {
    alert(
`
The Application suffered an internal server error. 
The maintainers of the application have been notified. 
Please feel free to contact ${contact} for assistance.
`
    )
    window.location = `/${script_root}`
};
