(function(){
    finalSubmit = document.getElementById("final-submit");

    //routine for when the user logs in
    finalSubmit.addEventListener("submit", async function(e) {
        e.preventDefault();
        const formData = new FormData(this);
        const response = await fetch(`/${script_root}/load`, {
            method: 'post',
            body: formData
        });
        console.log(response);
        const result = await response.json();
        console.log(result);
        
        alert(result.user_notification);

        finalSubmit.classList.add("hidden")

    })

})()