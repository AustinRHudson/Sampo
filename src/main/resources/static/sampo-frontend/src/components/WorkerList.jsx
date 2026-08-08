import WorkerCard from "./WorkerCard";

function WorkerList({ workers }) {

    const workerArray = Object.values(workers);

    return (
        <section className="section">

            <div className="section-header">

                <div>
                    <h2>Workers</h2>
                    <p>Currently registered workers</p>
                </div>

                <span className="worker-count">
                    {workerArray.length} workers
                </span>

            </div>

            <div className="worker-list">

                {workerArray.length === 0 ? (

                    <div className="empty-state">
                        <div className="empty-icon">
                            W
                        </div>

                        <h3>No workers registered</h3>

                        <p>
                            Start a worker application to see it here.
                        </p>
                    </div>

                ) : (

                    workerArray.map(worker => (
                        <WorkerCard
                            key={worker.id}
                            worker={worker}
                        />
                    ))

                )}

            </div>

        </section>
    );
}

export default WorkerList;