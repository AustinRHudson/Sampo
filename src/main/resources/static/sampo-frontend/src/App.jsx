import { useEffect, useState } from "react";

import { getWorkers, getJobs } from "./api";

import StatCard from "./components/StatCard";
import WorkerList from "./components/WorkerList";
import JobList from "./components/JobList";

import "./App.css";

function App() {

    const [workers, setWorkers] = useState({});
    const [jobs, setJobs] = useState([]);

    const [loading, setLoading] = useState(true);
    const [connectionError, setConnectionError] = useState(false);

    async function refreshData() {

        try {

            const workerData = await getWorkers();

            setWorkers(workerData);
            setConnectionError(false);

            try {
                const jobData = await getJobs();
                setJobs(jobData);
            } catch {
                // Jobs endpoint isn't implemented yet.
                setJobs([]);
            }

        } catch (error) {

            console.error(error);
            setConnectionError(true);

        } finally {

            setLoading(false);

        }
    }

    useEffect(() => {

        refreshData();

        const interval = setInterval(
            refreshData,
            5000
        );

        return () => clearInterval(interval);

    }, []);

    const workerArray = Object.values(workers);

    const onlineWorkers =
        workerArray.filter(
            worker => worker.status === "ONLINE"
        ).length;

    const runningJobs =
        jobs.filter(
            job => job.status === "RUNNING"
        ).length;

    const queuedJobs =
        jobs.filter(
            job => job.status === "QUEUED"
        ).length;

    const completedJobs =
        jobs.filter(
            job => job.status === "COMPLETED"
        ).length;

    return (
        <div className="app">

            <header className="header">

                <div className="brand">

                    <div className="logo">
                        M
                    </div>

                    <div>
                        <h1>Sampo</h1>
                        <p>
                            Distributed Job Execution Platform
                        </p>
                    </div>

                </div>

                <div className="connection">

                    <span
                        className={
                            connectionError
                                ? "connection-dot offline"
                                : "connection-dot"
                        }
                    />

                    {connectionError
                        ? "Scheduler Offline"
                        : "Scheduler Online"}

                </div>

            </header>


            <main className="dashboard">

                <section className="overview">

                    <StatCard
                        title="Workers"
                        value={onlineWorkers}
                        subtitle={`${workerArray.length} registered`}
                    />

                    <StatCard
                        title="Running"
                        value={runningJobs}
                        subtitle="Active jobs"
                    />

                    <StatCard
                        title="Queued"
                        value={queuedJobs}
                        subtitle="Waiting for workers"
                    />

                    <StatCard
                        title="Completed"
                        value={completedJobs}
                        subtitle="Successful jobs"
                    />

                </section>


                {loading ? (

                    <div className="loading">
                        Connecting to scheduler...
                    </div>

                ) : (

                    <>
                        <WorkerList workers={workers} />

                        <JobList jobs={jobs} />
                    </>

                )}

            </main>

        </div>
    );
}

export default App;