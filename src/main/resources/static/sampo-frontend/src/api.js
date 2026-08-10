const API_URL = "http://localhost:8080/workers";

export async function getWorkers() {
    const response = await fetch(`${API_URL}/list`);

    if (!response.ok) {
        throw new Error("Failed to fetch workers");
    }

    return response.json();
}

export async function getJobs() {
    const response = await fetch(`${API_URL}/jobs`);

    if (!response.ok) {
        throw new Error("Failed to fetch jobs");
    }

    return response.json();
}

export async function getSystemStatus() {
    const response = await fetch(`${API_URL}/status`);

    if (!response.ok) {
        throw new Error("Scheduler unavailable");
    }

    return response.json();
}