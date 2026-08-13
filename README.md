<div align="center">

<h1>Sampo</h1>

<p>
  <strong>Distributed Job Execution Platform</strong>
</p>

<p>
  Java • Spring Boot • Kafka • PostgreSQL • Docker • React
</p>

</div>

<hr>

<h2>About</h2>

<p>
Sampo is a distributed job execution platform that allows users to submit
applications as ZIP files and execute them inside isolated Docker containers.
Jobs are distributed to workers through Apache Kafka and their execution
status is stored in PostgreSQL.
</p>

<h2>Features</h2>

<ul>
  <li>Upload and execute jobs through a web interface</li>
  <li>Distributed job processing with Apache Kafka</li>
  <li>Dynamic worker creation and management</li>
  <li>Docker containerized job execution</li>
  <li>Configurable CPU and memory limits</li>
  <li>Persistent job history with PostgreSQL</li>
  <li>Job status and exit code tracking</li>
  <li>React dashboard for job submission and monitoring</li>
</ul>

<h2>Architecture</h2>

<pre>
              React
                │
                ▼
        Spring Boot Scheduler
           │           │
           ▼           ▼
      PostgreSQL      Kafka
                       │
              ┌────────┼────────┐
              ▼        ▼        ▼
           Worker   Worker   Worker
              │        │        │
              ▼        ▼        ▼
           Docker   Docker   Docker
</pre>

<h2>Job Flow</h2>

<ol>
  <li>User uploads a ZIP containing their application.</li>
  <li>Sampo creates a job and stores it in PostgreSQL.</li>
  <li>The job is placed into a Kafka queue.</li>
  <li>An available worker receives the job.</li>
  <li>The worker builds and runs the application in Docker.</li>
  <li>CPU and memory limits are applied to the container.</li>
  <li>The worker reports the result back to the scheduler.</li>
  <li>The job status and exit code are stored in PostgreSQL.</li>
</ol>

<h2>Example</h2>

<p>
A submitted job can contain a simple application and Dockerfile:
</p>

<pre>
testJob/
├── Dockerfile
└── main.py
</pre>

<p>
Sampo packages the job, distributes it to a worker, builds the Docker image,
and executes it in an isolated container.
</p>

<h2>Tech Stack</h2>

<table>
  <tr>
    <th>Component</th>
    <th>Technology</th>
  </tr>
  <tr>
    <td>Backend</td>
    <td>Java / Spring Boot</td>
  </tr>
  <tr>
    <td>Messaging</td>
    <td>Apache Kafka</td>
  </tr>
  <tr>
    <td>Database</td>
    <td>PostgreSQL / Hibernate</td>
  </tr>
  <tr>
    <td>Execution</td>
    <td>Docker</td>
  </tr>
  <tr>
    <td>Frontend</td>
    <td>React</td>
  </tr>
</table>

</div>
