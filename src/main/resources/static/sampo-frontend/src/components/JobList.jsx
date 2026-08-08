import JobCard from "./JobCard";

function JobList({ jobs }) {

    return (
        <section className="section">

            <div className="section-header">

                <div>
                    <h2>Recent Jobs</h2>
                    <p>Latest job executions</p>
                </div>

                <button className="secondary-button">
                    View All
                </button>

            </div>

            <div className="job-list">

                {jobs.length === 0 ? (

                    <div className="empty-state">
                        <h3>No jobs yet</h3>
                        <p>
                            Submitted jobs will appear here.
                        </p>
                    </div>

                ) : (

                    jobs.map(job => (
                        <JobCard
                            key={job.id}
                            job={job}
                        />
                    ))

                )}

            </div>

        </section>
    );
}

export default JobList;