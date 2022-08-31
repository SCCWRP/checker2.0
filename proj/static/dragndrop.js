/* 
Just a few simple lines of code to stop the default behavior on a drag and drop, 
in order to allow the drag and drop functionality to work
*/

(function(){
    const events = ["drag", "dragstart", "dragend", "dragover", "dragenter", "dragleave", "drop"]
    for (let i = 0; i < events.length; i++){
        document.querySelector('body').addEventListener(events[i], (e) => {
            e.stopPropagation();
            e.preventDefault();
        })
    }

})()


