CREATE OR REPLACE FUNCTION check_task_assignment_rules()
RETURNS TRIGGER AS $$
DECLARE
    v_conn TEXT := 'dbname=' || current_database();
    v_audit_sql TEXT;
    v_has_rights BOOLEAN;
    v_active_tasks INTEGER;
    v_max_tasks INTEGER;
    v_available_budget NUMERIC(12, 2);
    v_can_manage_budget BOOLEAN;
    v_employee_name VARCHAR(255);
    v_project_name VARCHAR(255);
    v_project_status VARCHAR(50);
BEGIN
    SELECT full_name INTO v_employee_name FROM task_manager.employee WHERE employee_id = NEW.employee_id;
    SELECT project_name, status INTO v_project_name, v_project_status FROM task_manager.project WHERE project_id = NEW.project_id;

    v_audit_sql := format('INSERT INTO task_manager.task_audit (task_id, operation_type, check_result, message) VALUES (%s, %L, %L, %L)',
                          NEW.task_id, 'INSERT', 'PENDING', 'Начата проверка условий назначения задачи');
    PERFORM dblink_exec(v_conn, v_audit_sql);

-- checking project for activity
    IF v_project_status != 'ACTIVE' THEN
        v_audit_sql := format('INSERT INTO task_manager.task_audit (task_id, operation_type, check_result, message) VALUES (%s, %L, %L, %L)',
                              NEW.task_id, 'INSERT', 'FAIL', 'Проект "' || v_project_name || '" не находится в активном статусе');
        PERFORM dblink_exec(v_conn, v_audit_sql);
        RAISE EXCEPTION 'Ошибка назначения задачи: проект "%" не активен.', v_project_name;
    END IF;

    SELECT can_assign_tasks, max_tasks_in_project, can_edit_budget
    INTO v_has_rights, v_max_tasks, v_can_manage_budget
    FROM task_manager.employee_project_rights
    WHERE employee_id = NEW.employee_id AND project_id = NEW.project_id;

-- checking employee rights
    IF NOT FOUND OR NOT v_has_rights THEN
        v_audit_sql := format('INSERT INTO task_manager.task_audit (task_id, operation_type, check_result, message) VALUES (%s, %L, %L, %L)',
                              NEW.task_id, 'INSERT', 'FAIL', 'Сотрудник не имеет прав на выполнение задач в проекте');
        PERFORM dblink_exec(v_conn, v_audit_sql);
        RAISE EXCEPTION 'Ошибка назначения задачи: сотрудник "%" не уполномочен выполнять задачи в проекте "%".', v_employee_name, v_project_name;
    END IF;

    SELECT COUNT(*) INTO v_active_tasks
    FROM task_manager.task
    WHERE employee_id = NEW.employee_id 
      AND project_id = NEW.project_id 
      AND status IN ('PENDING', 'IN_PROGRESS');
      
-- checking limit of acrivity task
    IF v_active_tasks >= v_max_tasks THEN
        v_audit_sql := format('INSERT INTO task_manager.task_audit (task_id, operation_type, check_result, message) VALUES (%s, %L, %L, %L)',
                              NEW.task_id, 'INSERT', 'FAIL', 'Превышен лимит активных задач');
        PERFORM dblink_exec(v_conn, v_audit_sql);
        RAISE EXCEPTION 'Ошибка назначения задачи: сотрудник "%" уже выполняет максимальное количество задач (%) в проекте "%".', v_employee_name, v_max_tasks, v_project_name;
    END IF;

    IF NEW.budget_required > 0 THEN
        SELECT COALESCE(total_budget - spent_budget, 0) INTO v_available_budget
        FROM task_manager.project WHERE project_id = NEW.project_id;

-- checking budget
        IF v_available_budget < NEW.budget_required THEN
            v_audit_sql := format('INSERT INTO task_manager.task_audit (task_id, operation_type, check_result, message) VALUES (%s, %L, %L, %L)',
                                  NEW.task_id, 'INSERT', 'FAIL', 'Недостаточно бюджета на проекте');
            PERFORM dblink_exec(v_conn, v_audit_sql);
            RAISE EXCEPTION 'Ошибка назначения задачи: в проекте "%" недостаточно бюджета (доступно: %, требуется: %).', v_project_name, v_available_budget, NEW.budget_required;
        END IF;

        IF NOT v_can_manage_budget AND NEW.budget_required > 100.00 THEN
            v_audit_sql := format('INSERT INTO task_manager.task_audit (task_id, operation_type, check_result, message) VALUES (%s, %L, %L, %L)',
                                  NEW.task_id, 'INSERT', 'FAIL', 'Сотрудник не уполномочен расходовать бюджет свыше установленного порога');
            PERFORM dblink_exec(v_conn, v_audit_sql);
            RAISE EXCEPTION 'Ошибка назначения задачи: сотрудник "%" не имеет полномочий на расходование бюджета в размере %. ', v_employee_name, NEW.budget_required;
        END IF;

        INSERT INTO task_manager.budget_reservation (task_id, project_id, reserved_amount, status)
        VALUES (NEW.task_id, NEW.project_id, NEW.budget_required, 'ACTIVE');

        UPDATE task_manager.project SET spent_budget = spent_budget + NEW.budget_required WHERE project_id = NEW.project_id;
    END IF;

    INSERT INTO task_manager.notification (employee_id, message, notification_type)
    VALUES (NEW.employee_id, 'Вам назначена задача: "' || NEW.title || '" в проекте "' || v_project_name || '".', 'TASK_ASSIGNMENT');

    v_audit_sql := format('INSERT INTO task_manager.task_audit (task_id, operation_type, check_result, message) VALUES (%s, %L, %L, %L)',
                          NEW.task_id, 'INSERT', 'PASS', 'Задача успешно назначена. Резерв бюджета: ' || COALESCE(NEW.budget_required, 0));
    PERFORM dblink_exec(v_conn, v_audit_sql);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE EXTENSION dblink;

CREATE or REPLACE TRIGGER trg_check_task_assignment
    BEFORE INSERT ON Task
    FOR EACH ROW
    EXECUTE FUNCTION check_task_assignment_rules();
