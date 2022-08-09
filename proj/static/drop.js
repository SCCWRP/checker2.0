{
    // if there was no application failure, we can proceed
    // Add drag and drop event listener only after the user signs in
    document.querySelector("body").addEventListener('drop', function(event){
        event.stopPropagation();
        event.preventDefault();
        document.querySelector('#file-submission-form').reset(); 
        document.getElementById("reload-button-container").classList.add("hidden");
        document.getElementById("excel-markup-download").classList.add('hidden');
        document.getElementById("final-submit-button-container").classList.add('hidden');
        const dropped_files = event.dataTransfer.files;
        document.querySelector('#file-submission-form input.form-control-file').files = dropped_files;
        document.querySelector('#file-submission-form').requestSubmit();
    });
}