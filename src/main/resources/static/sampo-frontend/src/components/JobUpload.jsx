import { useState } from "react";

export default function JobUpload() {
    const [file, setFile] = useState(null);
    const [cpuLimit, setCpuLimit] = useState("");
    const [memoryLimit, setMemoryLimit] = useState("");
    const [message, setMessage] = useState("");
    const [uploading, setUploading] = useState(false);

    const handleSubmit = async (e) => {
        e.preventDefault();

        if (!file) {
            setMessage("Please select a ZIP file.");
            return;
        }

        if (!file.name.toLowerCase().endsWith(".zip")) {
            setMessage("Only ZIP files are allowed.");
            return;
        }

        const formData = new FormData();

        formData.append("file", file);

        // Only send these if the user entered them
        if (cpuLimit !== "") {
            formData.append("cpuLimit", cpuLimit);
        }

        if (memoryLimit !== "") {
            formData.append("memoryLimitMb", memoryLimit);
        }

        try {
            setUploading(true);
            setMessage("");

            const response = await fetch("http://localhost:8080/workers/job", {
                method: "POST",
                body: formData,
            });

            if (!response.ok) {
                throw new Error(`Upload failed: ${response.status}`);
            }

            setMessage("Job successfully submitted!");

            // Reset form
            setFile(null);
            setCpuLimit("");
            setMemoryLimit("");

            // Reset file input
            document.getElementById("job-file").value = "";
        } catch (error) {
            console.error(error);
            setMessage("Failed to submit job.");
        } finally {
            setUploading(false);
        }
    };

    return (
        <div className="job-upload">
            <h2>Submit Job</h2>

            <form onSubmit={handleSubmit}>

                <div>
                    <label htmlFor="job-file">
                        Job ZIP
                    </label>

                    <input
                        id="job-file"
                        type="file"
                        accept=".zip"
                        onChange={(e) => setFile(e.target.files[0])}
                    />

                    {file && (
                        <p>
                            Selected: {file.name}
                        </p>
                    )}
                </div>

                <div>
                    <label htmlFor="cpu-limit">
                        CPU Limit
                        <span> (optional)</span>
                    </label>

                    <input
                        id="cpu-limit"
                        type="number"
                        min="0.00001"
                        step="0.00001"
                        value={cpuLimit}
                        onChange={(e) => setCpuLimit(e.target.value)}
                        placeholder="e.g. 1.0"
                    />

                    <small>
                        Maximum CPU cores available to the job
                    </small>
                </div>

                <div>
                    <label htmlFor="memory-limit">
                        Memory Limit
                        <span> (optional)</span>
                    </label>

                    <input
                        id="memory-limit"
                        type="number"
                        min="6"
                        step="1"
                        value={memoryLimit}
                        onChange={(e) => setMemoryLimit(e.target.value)}
                        placeholder="e.g. 512"
                    />

                    <small>
                        Maximum memory in MB
                    </small>
                </div>

                <button type="submit" disabled={uploading}>
                    {uploading ? "Submitting..." : "Submit Job"}
                </button>

                {message && (
                    <p>{message}</p>
                )}
            </form>
        </div>
    );
}