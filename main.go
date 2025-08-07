package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "fmt"
    "log"
    "os"
    "time"

    "github.com/IBM/sarama"
    _ "github.com/lib/pq"
    "github.com/robfig/cron/v3"
)

type Job struct {
    ID          int       `json:"id"`
    Name        string    `json:"name"`
    Source      string    `json:"source"`
    Filters     string    `json:"filters"`
    CronExpr    string    `json:"cron_expr"`
    Concurrency int       `json:"concurrency"`
    Active      bool      `json:"active"`
    CreatedAt   time.Time `json:"created_at"`
    UpdatedAt   time.Time `json:"updated_at"`
}

type ScrapingTask struct {
    JobID       int                    `json:"job_id"`
    Source      string                 `json:"source"`
    Filters     map[string]interface{} `json:"filters"`
    Concurrency int                    `json:"concurrency"`
    TaskID      string                 `json:"task_id"`
    Timestamp   time.Time              `json:"timestamp"`
}

type Scheduler struct {
    db       *sql.DB
    producer sarama.SyncProducer
    cron     *cron.Cron
}

func NewScheduler() (*Scheduler, error) {
    // Connect to database
    dbURL := os.Getenv("DATABASE_URL")
    if dbURL == "" {
        dbURL = "postgresql://postgres:postgres@postgres:5432/leadflowx?sslmode=disable"
    }

    db, err := sql.Open("postgres", dbURL)
    if err != nil {
        return nil, fmt.Errorf("failed to connect to database: %v", err)
    }

    // Test database connection
    if err := db.Ping(); err != nil {
        return nil, fmt.Errorf("failed to ping database: %v", err)
    }

    // Connect to Kafka
    kafkaBroker := os.Getenv("KAFKA_BROKER")
    if kafkaBroker == "" {
        kafkaBroker = "kafka:9092"
    }

    config := sarama.NewConfig()
    config.Producer.Return.Successes = true
    config.Producer.RequiredAcks = sarama.WaitForAll
    config.Producer.Retry.Max = 5

    producer, err := sarama.NewSyncProducer([]string{kafkaBroker}, config)
    if err != nil {
        return nil, fmt.Errorf("failed to create Kafka producer: %v", err)
    }

    // Initialize cron scheduler
    c := cron.New(cron.WithSeconds())

    return &Scheduler{
        db:       db,
        producer: producer,
        cron:     c,
    }, nil
}

func (s *Scheduler) LoadJobs() error {
    rows, err := s.db.Query(`
        SELECT id, name, source, filters, cron_expr, concurrency, active, created_at, updated_at 
        FROM scraping_jobs 
        WHERE active = true
    `)
    if err != nil {
        return fmt.Errorf("failed to load jobs: %v", err)
    }
    defer rows.Close()

    for rows.Next() {
        var job Job
        err := rows.Scan(
            &job.ID, &job.Name, &job.Source, &job.Filters, 
            &job.CronExpr, &job.Concurrency, &job.Active,
            &job.CreatedAt, &job.UpdatedAt,
        )
        if err != nil {
            log.Printf("Error scanning job row: %v", err)
            continue
        }

        // Schedule the job
        _, err = s.cron.AddFunc(job.CronExpr, func() {
            s.ExecuteJob(job)
        })
        if err != nil {
            log.Printf("Error scheduling job %d: %v", job.ID, err)
            continue
        }

        log.Printf("Scheduled job %d (%s) with cron expression: %s", job.ID, job.Name, job.CronExpr)
    }

    return rows.Err()
}

func (s *Scheduler) ExecuteJob(job Job) {
    log.Printf("Executing job %d (%s) - Source: %s", job.ID, job.Name, job.Source)

    // Parse filters
    var filters map[string]interface{}
    if err := json.Unmarshal([]byte(job.Filters), &filters); err != nil {
        log.Printf("Error parsing filters for job %d: %v", job.ID, err)
        return
    }

    // Create scraping task
    task := ScrapingTask{
        JobID:       job.ID,
        Source:      job.Source,
        Filters:     filters,
        Concurrency: job.Concurrency,
        TaskID:      fmt.Sprintf("%d_%d", job.ID, time.Now().Unix()),
        Timestamp:   time.Now(),
    }

    // Serialize task to JSON
    taskJSON, err := json.Marshal(task)
    if err != nil {
        log.Printf("Error serializing task for job %d: %v", job.ID, err)
        return
    }

    // Send to Kafka topic based on source
    topic := fmt.Sprintf("scraping.%s", job.Source)
    msg := &sarama.ProducerMessage{
        Topic: topic,
        Key:   sarama.StringEncoder(task.TaskID),
        Value: sarama.ByteEncoder(taskJSON),
    }

    partition, offset, err := s.producer.SendMessage(msg)
    if err != nil {
        log.Printf("Error sending task to Kafka for job %d: %v", job.ID, err)
        return
    }

    log.Printf("Task for job %d sent to Kafka topic %s (partition: %d, offset: %d)", 
        job.ID, topic, partition, offset)

    // Update job last_run timestamp
    _, err = s.db.Exec(
        "UPDATE scraping_jobs SET last_run = $1 WHERE id = $2",
        time.Now(), job.ID,
    )
    if err != nil {
        log.Printf("Error updating last_run for job %d: %v", job.ID, err)
    }
}

func (s *Scheduler) Start() error {
    log.Println("Starting LeadFlowX Scheduler...")

    // Load and schedule all active jobs
    if err := s.LoadJobs(); err != nil {
        return fmt.Errorf("failed to load jobs: %v", err)
    }

    // Start the cron scheduler
    s.cron.Start()
    log.Println("Scheduler started successfully")

    // Keep the scheduler running
    select {}
}

func (s *Scheduler) Stop() {
    log.Println("Stopping scheduler...")
    s.cron.Stop()
    s.producer.Close()
    s.db.Close()
    log.Println("Scheduler stopped")
}

func main() {
    scheduler, err := NewScheduler()
    if err != nil {
        log.Fatalf("Failed to create scheduler: %v", err)
    }

    // Graceful shutdown handling
    ctx := context.Background()
    _ = ctx // Use ctx for graceful shutdown if needed

    if err := scheduler.Start(); err != nil {
        log.Fatalf("Failed to start scheduler: %v", err)
    }
}
