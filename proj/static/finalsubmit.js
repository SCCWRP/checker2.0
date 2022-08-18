
const addFinalSubmitListener = () => {

    const finalSubmit = document.getElementById("final-submit");
    finalSubmit.addEventListener("submit", async function(e) {
        e.stopImmediatePropagation(); // form was submitting twice when the button was clicked
        e.preventDefault();
        
        document.getElementById("final-submit-button-container").style.display = 'none';
        document.querySelector(".submission-report-outer-container.after-submit").classList.add("hidden");
        document.getElementById("loader-gif-container").classList.remove("hidden");

        const formData = new FormData(this);
        const response = await fetch(`/${script_root}/load`, {
            method: 'post',
            body: formData
        });
        console.log(response);
        const result = await response.json();
        console.log(result);

        document.getElementById("loader-gif-container").style.display = 'none';
        
        
        // handling the case where there was a critical error
        if (result.critical_error) {
            // critical_error_handler defined in globals.js
            critical_error_handler(result.contact)
        } else {

            // here would be the case of a successful data load
            document.getElementById('after-successful-final-submit').classList.remove("hidden");

        }
    
    })
}

// simply adding an event listener to reload the page after the user successfully submits their data
// adding an event listener to the reload button
(function (){
    const reloadButtons = document.querySelectorAll('.btn-reload');
    for (let i = 0; i < reloadButtons.length; i++) {
        reloadButtons[i].addEventListener('click', function () {
            window.location = `/${script_root}`;
        })
    }

    // not like there are going to be more than one button for this, but eh, why not right?
    const sccwrpButtons = document.querySelectorAll('.btn-sccwrp');
    for (let i = 0; i < sccwrpButtons.length; i++) {
        sccwrpButtons[i].addEventListener('click', function () {
            window.location = `https://www.sccwrp.org/`;
        })
    }
    
})()