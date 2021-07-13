
const addFinalSubmitListener = () => {

    const finalSubmit = document.getElementById("final-submit");
    finalSubmit.addEventListener("submit", async function(e) {
        e.preventDefault();
        finalSubmit.style.display = 'none';
        const formData = new FormData(this);
        const response = await fetch(`/${script_root}/load`, {
            method: 'post',
            body: formData
        });
        console.log(response);
        const result = await response.json();
        console.log(result);
        
        // handling the case where there was a critical error
        if (result.critical_error) {
            // critical_error_handler defined in globals.js
            critical_error_handler(result.contact)
        } else {

            // here would be the case of a successful data load
            alert(result.user_notification);
        }
        
        window.location = `/${script_root}`
    
    })
}