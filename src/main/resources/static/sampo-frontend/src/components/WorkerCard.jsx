function WorkerCard({ worker }) {

    const status =
        worker.status?.toLowerCase() || "unknown";

    return (
        <div className="worker-card">

            <div className="worker-info">

                <div className="worker-icon">
                    W
                </div>

                <div>
                    <h3>{worker.id}</h3>

                    <p>
                        {worker.host}:{worker.port}
                    </p>
                </div>

            </div>

            <div className="worker-details">

                <span className={`status ${status}`}>
                    <span className="status-dot"></span>
                    {worker.status}
                </span>

                <span className="job-count">
                    {worker.activeJobs ?? 0} active jobs
                </span>

            </div>

        </div>
    );
}

export default WorkerCard;