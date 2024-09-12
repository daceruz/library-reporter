async function startReports(){
    if (!confirm("WARNING: The library reporter will make the Bixal Library Website run slower than usual. Are you sure you want to run the reports?")) {
        return
    }

    const button = document.getElementById('run-button');
    button.style.display="none";

    const progress_display =  document.getElementById('progress-display')

    progress_display.innerHTML += '<h3>Creating Dictionaries</h3>';

    progress_display.innerHTML += '<p>Processing User_ID to Name and User_ID to Email Dictionaries:</p> ';
    progress_display.innerHTML += '<progress id="p0" value="0" max="100"></progress>';

    progress_display.innerHTML += '<p>Processing Shelf_ID to Slug, Shelf_ID to Name, and Shelf_ID to Owner_ID Dictionaries:</p> ';
    progress_display.innerHTML += '<progress id="p1" value="0" max="100"></progress>';

    progress_display.innerHTML += '<p>Processing Book_ID to Shelf_ID Dictionary:</p> ';
    progress_display.innerHTML += '<progress id="p2" value="0" max="100"></progress>';

    progress_display.innerHTML += '<p>Processing Book_ID to Slug, Book_ID to Name, and Book_ID to Owner_ID Dictionaries:</p> ';
    progress_display.innerHTML += '<progress id="p3" value="0" max="100"></progress>';

    progress_display.innerHTML += '<p>Processing Chapter_ID to Slug, Chapter_ID to Name, Chapter_ID to Owner_ID, and Chapter_ID to Book_ID Dictionaries:</p> ';
    progress_display.innerHTML += '<progress id="p4" value="0" max="100"></progress>';

    progress_display.innerHTML += '<p>Processing Page_ID to Slug, Page_ID to Name, and Page_ID to Book_ID Dictionaries:</p> ';
    progress_display.innerHTML += '<progress id="p5" value="0" max="100"></progress>';

    const progress_bar_0 = document.getElementById('p0')
    const progress_bar_1 = document.getElementById('p1')
    const progress_bar_2 = document.getElementById('p2')
    const progress_bar_3 = document.getElementById('p3')
    const progress_bar_4 = document.getElementById('p4')
    const progress_bar_5 = document.getElementById('p5')

    // Starting the Setup
    fetch('/startsetup', {
        method: 'POST'
    });

    while (progress_bar_5.value < 99.99) {
        try {
            const response = await fetch('/progress');
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            const data = await response.json();
            if ('p0' in data) {
                progress_bar_0.value = data.p0;
            }
            if ('p1' in data) {
                progress_bar_1.value = data.p1;
            }
            if ('p2' in data) {
                progress_bar_2.value = data.p2;
            }
            if ('p3' in data) {
                progress_bar_3.value = data.p3;
            }
            if ('p4' in data) {
                progress_bar_4.value = data.p4;
            }
            if ('p5' in data) {
                progress_bar_5.value = data.p5;
            }
            // Wait for 1 second before the next iteration
            await new Promise(resolve => setTimeout(resolve, 1000));
        } catch (error) {
            console.error('There was a problem with the fetch operation:', error);
        }
    }

    progress_display.innerHTML += '<h3>Dictionary Creation Complete!</h3>';


    progress_display.innerHTML += '<h3>Creating Reports (ETA: 8 Minutes)...</h3>';

    progress_display.innerHTML += '<p>Gathering Tags For All Pages:</p> ';
    progress_display.innerHTML += '<progress id="p6" value="0" max="100"></progress>';

    progress_display.innerHTML += '<p>Formatting All Pages:</p> ';
    progress_display.innerHTML += '<progress id="p7" value="0" max="100"></progress>';

    progress_display.innerHTML += '<p>Formatting All Attachments:</p> ';
    progress_display.innerHTML += '<progress id="p8" value="0" max="100"></progress>';

    progress_display.innerHTML += '<p>Formatting All Books:</p> ';
    progress_display.innerHTML += '<progress id="p9" value="0" max="100"></progress>';

    progress_display.innerHTML += '<p>Filtering All Books for Duplicates:</p> ';
    progress_display.innerHTML += '<progress id="p10" value="0" max="100"></progress>';

    progress_display.innerHTML += '<p>Filtering All Books For Any That Are Unshelved:</p> ';
    progress_display.innerHTML += '<progress id="p11" value="0" max="100"></progress>';

    progress_display.innerHTML += '<p>Formatting All Chapters:</p> ';
    progress_display.innerHTML += '<progress id="p12" value="0" max="100"></progress>';

    progress_display.innerHTML += '<p>Filtering All Pages for Duplicates:</p> ';
    progress_display.innerHTML += '<progress id="p13" value="0" max="100"></progress>';

    progress_display.innerHTML += '<p>Formatting All Shelves:</p> ';
    progress_display.innerHTML += '<progress id="p14" value="0" max="100"></progress>';

    progress_display.innerHTML += '<p>Formatting All Users:</p> ';
    progress_display.innerHTML += '<progress id="p15" value="0" max="100"></progress>';

    const progress_bar_6 = document.getElementById('p6')
    const progress_bar_7 = document.getElementById('p7')
    const progress_bar_8 = document.getElementById('p8')
    const progress_bar_9 = document.getElementById('p9')
    const progress_bar_10 = document.getElementById('p10')
    const progress_bar_11 = document.getElementById('p11')
    const progress_bar_12 = document.getElementById('p12')
    const progress_bar_13 = document.getElementById('p13')
    const progress_bar_14 = document.getElementById('p14')
    const progress_bar_15 = document.getElementById('p15')

    // Starting the Reports
    fetch('/startreports', {
        method: 'POST'
    });

    while (progress_bar_15.value < 99.99) {
        try {
            const response = await fetch('/progress');
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            const data = await response.json();

            if ('p6' in data) {
                progress_bar_6.value = data.p6;
            }
            if ('p7' in data) {
                progress_bar_7.value = data.p7;
            }
            if ('p8' in data) {
                progress_bar_8.value = data.p8;
            }
            if ('p9' in data) {
                progress_bar_9.value = data.p9;
            }
            if ('p10' in data) {
                progress_bar_10.value = data.p10;
            }
            if ('p11' in data) {
                progress_bar_11.value = data.p11;
            }
            if ('p12' in data) {
                progress_bar_12.value = data.p12;
            }
            if ('p13' in data) {
                progress_bar_13.value = data.p13;
            }
            if ('p14' in data) {
                progress_bar_14.value = data.p14;
            }
            if ('p15' in data) {
                progress_bar_15.value = data.p15;
            }
            // Wait for 1 second before the next iteration
            await new Promise(resolve => setTimeout(resolve, 1000));
        } catch (error) {
            console.error('There was a problem with the fetch operation:', error);
        }
    }

    progress_display.innerHTML += '<h3>Report Creation Complete!</h3>';
    const date = new Date();
    progress_display.innerHTML += `<a href="/download/library-report.xlsx" download><button>Download Report</button></a>`

}