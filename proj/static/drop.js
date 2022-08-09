{
    // if there was no application failure, we can proceed
    // Add drag and drop event listener only after the user signs in
    document.querySelector("body").addEventListener('drop', function(event){
        event.stopPropagation();
        event.preventDefault();
        const dropped_files = event.dataTransfer.files;
        document.querySelector('#file-submission-form input.form-control-file').files = dropped_files;
        document.querySelector('#file-submission-form').requestSubmit();
    });
}