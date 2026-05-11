CREATE SCHEMA IF NOT EXISTS task_manager;
SET search_path TO task_manager;
CREATE TABLE Employee (
    employee_id SERIAL PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    position VARCHAR(100) NOT NULL,
    department VARCHAR(100),
    max_active_tasks INTEGER NOT NULL DEFAULT 5 CHECK (max_active_tasks >= 0)
);

CREATE TABLE Project (
    project_id SERIAL PRIMARY KEY,
    project_name VARCHAR(255) NOT NULL,
    description TEXT,
    total_budget NUMERIC(12, 2) NOT NULL CHECK (total_budget >= 0),
    spent_budget NUMERIC(12, 2) NOT NULL DEFAULT 0 CHECK (spent_budget >= 0),
    status VARCHAR(50) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'ON_HOLD', 'COMPLETED', 'CANCELLED')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (spent_budget <= total_budget)
);

CREATE TABLE Task (
    task_id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    employee_id INTEGER NOT NULL REFERENCES Employee(employee_id) ON DELETE CASCADE,
    project_id INTEGER NOT NULL REFERENCES Project(project_id) ON DELETE CASCADE,
    priority VARCHAR(20) NOT NULL CHECK (priority IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    budget_required NUMERIC(10, 2) DEFAULT 0 CHECK (budget_required >= 0),
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deadline DATE
);

CREATE TABLE Employee_Project_Rights (
    right_id SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL REFERENCES Employee(employee_id) ON DELETE CASCADE,
    project_id INTEGER NOT NULL REFERENCES Project(project_id) ON DELETE CASCADE,
    can_assign_tasks BOOLEAN NOT NULL DEFAULT FALSE,
    can_edit_budget BOOLEAN NOT NULL DEFAULT FALSE,
    max_tasks_in_project INTEGER NOT NULL DEFAULT 3 CHECK (max_tasks_in_project >= 0),
    budget NUMERIC(12, 2) DEFAULT 100.00,
    UNIQUE(employee_id, project_id)
);

CREATE TABLE Notification (
    notification_id SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL REFERENCES Employee(employee_id) ON DELETE CASCADE,
    message TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_read BOOLEAN NOT NULL DEFAULT FALSE,
    notification_type VARCHAR(50) NOT NULL DEFAULT 'TASK_ASSIGNMENT'
);

CREATE TABLE Task_Audit (
    audit_id SERIAL PRIMARY KEY,
    task_id INTEGER,
    operation_type VARCHAR(20) NOT NULL,
    check_result VARCHAR(20) NOT NULL CHECK (check_result IN ('PASS', 'FAIL', 'PENDING')),
    message TEXT,
    checked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    checked_by VARCHAR(100) DEFAULT 'SYSTEM_TRIGGER'
);

CREATE TABLE Budget_Reservation (
    reservation_id SERIAL PRIMARY KEY,
    task_id INTEGER NOT NULL,
    project_id INTEGER NOT NULL REFERENCES Project(project_id) ON DELETE CASCADE,
    reserved_amount NUMERIC(10, 2) NOT NULL CHECK (reserved_amount > 0),
    reserved_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'COMMITTED', 'CANCELLED')),
    committed_at TIMESTAMP
);
