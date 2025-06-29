-- 1. Процедура обновляющая почасовую ставку сотрудников на определенный процент. 
-- В данном случае ставка не может быть менее 500 единиц
/*
На вход процедура принимает строку в формате json:

[
    -- uuid сотрудника                                      процент изменения почасовой ставки
    {"employee_id": "6bfa5e20-918c-46d0-ab18-54fc61086cba", "rate_change": 10}, 
    {"employee_id": "5a6aed8f-8f53-4931-82f4-66673633f2a8", "rate_change": -5}
]

*/

CREATE OR REPLACE PROCEDURE update_employees_rate (r json)
LANGUAGE plpgsql
AS $$
	DECLARE
		_rate int;
		_rec JSON;
	BEGIN 
		FOR _rec IN SELECT JSON_ARRAY_ELEMENTS(r)
		LOOP
		SELECT
			ROUND((rate + rate * (_rec ->> 'rate_change')::int / 100)) INTO _rate FROM employees;
			IF _rate < 500 THEN UPDATE employees SET rate = 500 WHERE id = (_rec ->> 'employee_id')::uuid;
			ELSE UPDATE employees SET rate = _rate WHERE id = (_rec ->> 'employee_id')::uuid;
			END IF;
	 	END LOOP;
	END;
$$;

------------------------------------------------------------------------------------------------------------------------

-- Процедура принимает процент индексации (p) в целочисленном виде и увеличивает зарплату сотрудников на заданный параметр.
-- Для сотрудников, чья зарплата ниже средней относительно всех будет начислено (p+2)%
CREATE OR REPLACE PROCEDURE indexing_salary (in p int)
LANGUAGE plpgsql
AS $$
	DECLARE 
		_rec record;
		_avg_salary int;
	BEGIN
		-- ср. зарплату среди всех сотрудников записываем в переменную _avg_salary
		SELECT ROUND(AVG(rate), 0)::int FROM employees	INTO _avg_salary;
		
		FOR _rec IN (SELECT id, rate FROM employees GROUP BY id, rate) 
		LOOP
			CASE 
				WHEN _rec.rate < _avg_salary THEN UPDATE employees SET rate = ROUND(_rec.rate * (1 + (p::numeric / 100 + 0.02)), 0) WHERE id = _rec.id;
				ELSE UPDATE employees SET rate = ROUND(_rec.rate * (1 + (p::numeric / 100)), 0) WHERE id = _rec.id;
			END CASE;
		END LOOP;
	END
$$;
/*
Пример вызова процедуры
CALL indexing_salary(5);  -- для индексации зарплаты на 5%
*/

------------------------------------------------------------------------------------------------------------------------

/*
Процедура логирования завершения проекта в системе учета. 
Завершая проект, нужно сделать два действия в системе учёта:
Изменить значение поля is_active в записи проекта на false — чтобы рабочее время по этому проекту больше не учитывалось.
Посчитать бонус, если он есть — то есть распределить неизрасходованное время между всеми членами команды проекта.
Неизрасходованное время — это разница между временем, которое выделили на проект (estimated_time), и фактически потраченным. 
Если поле estimated_time не задано, бонусные часы не распределятся. Если отработанных часов нет — расчитывать бонус не нужно.

Разберёмся с бонусом. 
Если в момент закрытия проекта estimated_time:
1. не NULL,
2. больше суммы всех отработанных над проектом часов,
то всем членам команды проекта начисляют бонусные часы.

Размер бонуса считаем как: 75% от сэкономленных часов делим на количество участников проекта, но не более 16 бонусных часов на 1 сотрудника.
Дробные значения округляем в меньшую сторону. Рабочие часы заносят в логи с текущей датой. 
Например, если на проект запланировали 100 часов, а сделали его за 30 — 3/4 от сэкономленных 70 часов распределится бонусом между участниками проекта.
Если проект уже закрыт, процедура вернет ошибку без начисления бонусных часов.
*/

CREATE OR REPLACE PROCEDURE close_project (pr_id uuid)
LANGUAGE plpgsql
AS $$
	DECLARE
		_est_time int;
		_total_work_hours int;
		_employees_col int;
		_total_bonus int := 0;
	BEGIN
		IF (SELECT COUNT(*) FROM (SELECT is_active FROM projects WHERE is_active = 'false' AND id = pr_id)) = 1 THEN RAISE EXCEPTION 'Проект закрыт';
			ELSE UPDATE projects SET is_active = 'false' WHERE id = pr_id;
		END IF;
		
	SELECT estimated_time FROM projects WHERE id = pr_id INTO _est_time;
	
	SELECT SUM(l.work_hours) FROM logs l WHERE l.project_id = pr_id INTO _total_work_hours;
	
	SELECT COUNT(DISTINCT l.employee_id) FROM logs l WHERE l.project_id = pr_id INTO _employees_col;

	IF 	(_total_work_hours != 0) AND
		(_est_time IS NOT NULL)	AND
		(_est_time > _total_work_hours)
	THEN _total_bonus = _total_bonus +  FLOOR( ( ( (_est_time - _total_work_hours) * 3)::numeric(18, 4) / 4) / _employees_col )::int;
	END IF;

	IF _total_bonus > 16 THEN _total_bonus = 16;
	ELSE END IF;

	INSERT INTO logs (employee_id, project_id, work_date, work_hours)
	SELECT DISTINCT l.employee_id, project_id, CURRENT_DATE, _total_bonus FROM logs l WHERE l.project_id = pr_id; 
	END
$$;

/*
Пример вызова процедуры
CALL close_project('4abb5b99-3889-4c20-a575-e65886f266f9');
SELECT * FROM projects LEFT JOIN logs ON projects.id = logs.project_id WHERE name LIKE 'Навигатор';
SELECT * FROM projects;
*/

------------------------------------------------------------------------------------------------------------------------

/* 
Процедура для логирования отработанных часов сотрудниками.

Процедура принимает id сотрудника, id проекта, дату и отработанные часы и вносит данные в таблицу logs.

Если проект завершён, добавить логи невозможно - процедура вернет ошибку 'Проект закрыт. Нельзя внести данные.'.

Количество залогированных часов может быть строго в диапазоне от 1 до 24 включительно — нельзя внести менее 1 часа или больше 24.
Если количество часов выходит за эти пределы - выведется предупреждение о недопустимых данных и выполнение процедуры остановится.

Запись помечается флагом required_review, если:
- залогированно более 16 часов за один день;
- запись внесена будущим числом;
- запись внесена более ранним числом, чем на неделю назад от текущего дня - например, если сегодня 24.05.2025, все записи старше 17.05.2025 пометятся флагом.
*/

CREATE OR REPLACE PROCEDURE log_work (in emp_id uuid, in pr_id uuid, in w_date date, in w_hours int)
LANGUAGE plpgsql
AS $$
	DECLARE
		_req_review bool = 'false';
		_total_work_hours int;
		_counter int = 0;
	BEGIN 
		IF EXISTS(SELECT * FROM projects WHERE id = pr_id AND is_active = 'false') THEN RAISE EXCEPTION 'Проект закрыт. Нельзя внести данные.';
		RETURN;

		ELSIF w_hours < 1 OR w_hours > 24 THEN RAISE EXCEPTION 'Невозможно внести менее 1 часа или более 24 часов.';
		RETURN;
		ELSE END IF;

		-- переменная, хранящая сумму часов отработанную сотрудников по проекту на дату
		SELECT SUM(work_hours) FROM logs INTO _total_work_hours WHERE employee_id = emp_id AND work_date = w_date;

		-- счетчик кол-ва залогированных записей сотрудником на дату
		SELECT COUNT(*) FROM logs INTO _counter WHERE employee_id = emp_id AND work_date = w_date;

		-- в случае если заносится первый лог по проекту присваиваем кол-во отработанных часов вместо NULL
		IF _total_work_hours IS NULL THEN _total_work_hours = w_hours;
		ELSE END IF;

		-- проверяем условия на установку флага required_review
		IF 
			_total_work_hours > 16 OR
			-- для проверки если вносимая запись превысит лимит в 16 часов
			( (_total_work_hours + w_hours) > 16 AND (_counter >= 1) ) OR
			w_date > CURRENT_DATE OR
			w_date < (CURRENT_DATE - INTERVAL '7 days')
		THEN
			_req_review = 'true';
		ELSE END IF;

		IF
			_req_review = 'true' 
		-- меняем статус ревью для ранее внесенных записей
		THEN 
			UPDATE logs SET required_review = _req_review WHERE employee_id = emp_id AND work_date = w_date; 
		ELSE END IF;
		
		-- записываем обработанные данные в таблицу logs
		INSERT INTO logs (employee_id, project_id, work_date, work_hours, required_review)
		VALUES (emp_id, pr_id, w_date, w_hours, _req_review);
	END
$$;

/*
Пример вызова процедуры
CALL log_work(
    'b15bb4c0-1ee1-49a9-bc58-25a014eebe36', -- employee uuid
    '778e5574-45ec-4be0-91eb-579146273232', -- project uuid
    '2025-04-05',                           -- work date
    1                                      -- worked hours, меняем параметр чтобы увидеть как поведет себя процедура пре превышении норма часов.
); 

SELECT * FROM logs WHERE employee_id = 'b15bb4c0-1ee1-49a9-bc58-25a014eebe36' AND work_date = '2025-04-05' ORDER BY work_date DESC;
*/

------------------------------------------------------------------------------------------------------------------------

/*
Триггерная функция и триггер для фиксации изменений почасовой ставки сотрудников. Создание аудита изменений.
*/

-- Создаем таблицу для хранения изменений на дату
CREATE TABLE IF NOT EXISTS employee_rate_history (
	id bigserial PRIMARY KEY,
	employee_id uuid,
	-- аналогично первой процедуры в данном проекте учитываем, что зарплата не может быть ниже 500 единиц
	rate int CHECK (rate >= 500),
	from_date DATE DEFAULT CURRENT_DATE,
	FOREIGN KEY (employee_id) REFERENCES employees(id)
);

-- Записываем в таблицу текущие данные по сотрудникам за текущий год 
INSERT INTO employee_rate_history (employee_id, rate, from_date)
SELECT DISTINCT
	id AS employee_id,
	rate,
	'2025-01-01'::date AS from_date
FROM
	employees;

-- создаем триггерную функцию для записи новых данных отличных от старых в созданную таблицу employee_rate_history
CREATE OR REPLACE FUNCTION save_employee_rate_history()
RETURNS trigger
LANGUAGE plpgsql
AS $$
	BEGIN
		IF OLD.rate IS DISTINCT FROM NEW.rate 
		THEN
			INSERT INTO employee_rate_history (employee_id, rate, from_date)
			VALUES (NEW.id, NEW.rate, CURRENT_DATE);
		END IF;
	RETURN NULL;
	END
$$;

-- создаем триггер срабатывания функции
CREATE OR REPLACE TRIGGER change_employee_rate
AFTER INSERT OR UPDATE ON employees
FOR EACH ROW
EXECUTE FUNCTION save_employee_rate_history();

/* 
Для проверки работы триггера
SELECT * FROM employee_rate_history ORDER BY from_date DESC;
UPDATE employees SET rate = 1000 WHERE id = '972fe41a-76f7-4d6d-a29c-086064bf6c75';
SELECT * FROM employees;
TRUNCATE TABLE employee_rate_history RESTART IDENTITY;
*/
------------------------------------------------------------------------------------------------------------------------

/*
Функция принимает id проекта и возвращает таблицу с id сотрудников, которые залогировали максимальное количество часов в указанном проекте. 
Результирующая таблица состоит из двух полей: кода сотрудника и количества часов, отработанных на проекте.
*/

CREATE OR REPLACE FUNCTION best_project_workers(in pr_id uuid)
RETURNS TABLE (employee uuid, work_hours bigint)
LANGUAGE plpgsql
AS $$
	BEGIN
	RETURN QUERY
	(
		-- в итоговую таблицу выводится только код сотрудника и кол-во залогированных часов
		SELECT
			fn_q.emp_id,
			fn_q.total_hours 
		FROM
		(
			-- оставляем топ-3 сотрудника по залогированным часам
			SELECT
				q2.project_id project_id,
	 		   	q2.employee_id emp_id,
	   		   	q2.th total_hours
			FROM
			(
				-- ранжируем кол-во отработанных часов по сотрудникам на проекте
				SELECT
					q.project_id project_id,
	   		   		q.employee_id employee_id,
	   		   		q.total_hours th,
			   		ROW_NUMBER() OVER (PARTITION BY q.project_id ORDER BY q.total_hours DESC) AS rn
				FROM
				(
					-- отбираем проект, код сотрудника и кол-во залогированных часов на проекте
					SELECT
						project_id,
						employee_id,
						SUM(l.work_hours) AS total_hours
					FROM
						logs l
					WHERE
						project_id = pr_id
					GROUP BY 
						project_id,
						employee_id
			 	) AS q
	  		) AS q2
			WHERE rn < 4
		) AS fn_q
	);
	END
$$;

/*
Пример вызова функции
SELECT employee, work_hours FROM best_project_workers('2dfffa75-7cd9-4426-922c-95046f3d06a0');
*/

------------------------------------------------------------------------------------------------------------------------

/*
Процедура суммирует все залогированные часы за определённый месяц и умножает на актуальную почасовую ставку сотрудника.
Исключения — записи с флажками required_review и is_paid.
Если суммарно по всем проектам сотрудник отработал более 160 часов в месяц, все часы свыше 160 оплатят с коэффициентом 1.25.
Результирующая таблица возвращает код сотрудника, имя, кол-во отработанных часов и зарплату.
*/

CREATE OR REPLACE FUNCTION calculate_month_salary(in p_begin date, in p_end date)
RETURNS TABLE (id uuid, employee text, worked_hours int, salary int)
LANGUAGE plpgsql
AS $$
	BEGIN 
	RETURN QUERY
	(
	 SELECT q2.e, q2.n, q2.wh, q2.salary
	 FROM
	 (
	 	-- рассчитываем зарплату
		SELECT
			q.e_id e,
			q.n n,
			q.work_hours wh,
			q.rate,
			CASE 
				WHEN q.work_hours > 160 THEN ROUND(((q.work_hours - 160) * q.rate * 1.25) + 160 * q.rate)::int
				ELSE (q.work_hours * q.rate)
			END AS salary
		FROM
		(
			-- отбираем записи за указанный период и исключаем из них уже выплаченные и требующие ревью
			SELECT
				l.employee_id AS e_id,
				e.name AS n,
				e.rate AS rate,
				SUM(l.work_hours)::int AS work_hours
			FROM
				logs l
			LEFT JOIN
				employees e ON l.employee_id = e.id 
			WHERE
				NOT is_paid AND
				NOT	required_review AND
				l.work_date BETWEEN p_begin AND p_end
			GROUP BY
				e_id,
				n,
				rate
		) AS q
	  ) AS q2
	);
	END
$$;

/*
Пример вызова функции
SELECT * FROM calculate_month_salary('2025-04-01'::date, '2025-04-30'::date);
*/