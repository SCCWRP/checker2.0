const tabs = (tabClass, tabIDs) => {
    /*
    tabClass should be the name of the tab class, but the headers need -header appended to the class name
    the body should have -body appended to it

    tabIds should be a mapping, of tab header ids to the corresponding body id
    It should be an object, key value pairs
    */

    const tabHeaders = Array.from(document.getElementsByClassName(`${tabClass}-header`));
    const tabBodies = Array.from(document.getElementsByClassName(`${tabClass}-body`));

    // hide all tab bodies except the first one
    for (let i = 0; i < tabBodies.length; i++) {
        if (i === 0) {
            tabBodies[i].classList.remove('hidden');
            tabHeaders[i].classList.add("active");
        } else {
            tabBodies[i].classList.add('hidden');

        }
    }

    tabHeaders.map(t => {
        t.addEventListener('click',function(){
            tabHeaders.map(x => x.classList.remove('active'));
            tabBodies.map(x => x.classList.add('hidden'));
            this.classList.add('active');
            let bodyID = tabIDs[this.getAttribute('id')];
            document.getElementById(bodyID).classList.remove('hidden');
        })
    })
}
tabs(
    tabClass = 'submission-info-tab', 
    tabIDs = {
        'submission-info-header':'submission-info-body',
        'errors-report-header':'errors-report-body',
        'warnings-report-header':'warnings-report-body',
        'map-report-header':'map-report-body'
    }
)

