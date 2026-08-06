document.getElementById("workerForm").addEventListener("submit", async (event) => {

    event.preventDefault();

    const worker = {
        id: document.getElementById("id").value,
        host: document.getElementById("host").value,
        port: parseInt(document.getElementById("port").value),
        status: document.getElementById("status").value
    };

    const response = await fetch("/workers/register", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify(worker)
    });

    const message = document.getElementById("message");

    if (response.ok) {
        message.textContent = "✅ Worker registered successfully!";
        message.style.color = "lightgreen";
    } else {
        message.textContent = "❌ Failed to register worker.";
        message.style.color = "salmon";
    }

});