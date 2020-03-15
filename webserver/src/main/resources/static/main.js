let stompClient = null;
const table = document.getElementById("table");
const lap = document.getElementById("lap");
const name = document.getElementById("race-name");
const circuit = document.getElementById("race-circuit");
const date = document.getElementById("race-date");

const mapping = {}

function newTimes(lapTimes) {
    lap.innerText = lapTimes.lap;
    times = lapTimes.times;
    for (let i = 0; i < times.length; i++) {
        generateRow(i + 1, times[i].driver, times[i].lastLap, times[i].total, lapTimes.lap);
    }
}

function generateRow(position, driver, lastLap, total, lap) {
    let row;
    let newRow;

    let driverCell;
    let lastLapCell;
    let totalCell;
    let lapCell;

    const noWhitespaceDriver = driver.replace(/\s+/g, '');

    if (document.getElementById("driver-" + noWhitespaceDriver) === null) {
        row = document.createElement("tr");
        row.id = "driver-" + noWhitespaceDriver;
        row.classList.add("node");
        row.classList.add(noWhitespaceDriver);
        driverCell = document.createElement("td");
        driverCell.id = "driver:" + noWhitespaceDriver + ":driver";
        driverCell.classList.add("__name");
        lastLapCell = document.createElement("td");
        lastLapCell.id = "driver:" + noWhitespaceDriver + ":lastLap";
        lastLapCell.classList.add("__last");
        totalCell = document.createElement("td");
        totalCell.id = "driver:" + noWhitespaceDriver + ":total";
        totalCell.classList.add("__over");
        lapCell = document.createElement("td");
        lapCell.id = "driver:" + noWhitespaceDriver + ":lap";
        lapCell.classList.add("__lap");
        row.appendChild(driverCell);
        row.appendChild(lastLapCell);
        row.appendChild(totalCell);
        row.appendChild(lapCell);
        table.appendChild(row)
    } else {
        driverCell = document.getElementById("driver:" + noWhitespaceDriver + ":driver")
        lastLapCell = document.getElementById("driver:" + noWhitespaceDriver + ":lastLap")
        totalCell = document.getElementById("driver:" + noWhitespaceDriver + ":total")
        lapCell = document.getElementById("driver:" + noWhitespaceDriver + ":lap")
    }

    driverCell.innerText = driver;
    totalCell.innerText = total;
    if (lastLap !== null) {
        lastLapCell.innerText = lastLap;
        lapCell.innerText = lap;
    }
    mapping[driver] = position;
    anime({
      targets: "." + noWhitespaceDriver,
      translateY: ( mapping[driver] - 1 ) * 27,
      duration: 500
    });
}

const socket = new SockJS("http://localhost:8080/websocket");
stompClient = Stomp.over(socket);
stompClient.connect({}, function (frame) {
    console.log("Connected: " + frame);

    stompClient.subscribe("/times/lap", function (test) {
        newTimes(JSON.parse(test.body))
    });
});
