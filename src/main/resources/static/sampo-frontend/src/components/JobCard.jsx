function JobCard({ job }) {

    const status =
        job.status?.toLowerCase() || "unknown";

    return (
        <div className="job-card">

            <div className="job-id">
                #{job.id}
            </div>

            <div className="job-language">
                {job.language}
            </div>

            <span className={`job-status ${status}`}>
                {job.status}
            </span>

            <div className="job-worker">
                {job.workerId || "—"}
            </div>

        </div>
    );
}

export default JobCard;