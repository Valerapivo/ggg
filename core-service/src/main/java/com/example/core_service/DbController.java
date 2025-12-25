package com.example.core_service;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.math.BigDecimal;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class DbController {

    private final NamedParameterJdbcTemplate jdbc;

    public DbController(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // Валидация имени таблицы/представления
    private void validateTableName(String table) {
        if (!table.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("Invalid table name: " + table);
        }
    }

    // Получить все данные из таблицы
    @GetMapping("/tables/{table}")
    public List<Map<String, Object>> getTable(@PathVariable String table) {
        validateTableName(table);
        String sql = "SELECT * FROM " + table + " ORDER BY id";
        return jdbc.queryForList(sql, new HashMap<>());
    }

    // Получить данные из представления
    @GetMapping("/views/{view}")
    public List<Map<String, Object>> getView(@PathVariable String view) {
        validateTableName(view);
        String sql = "SELECT * FROM " + view;
        return jdbc.queryForList(sql, new HashMap<>());
    }

    // Добавить новую запись в таблицу
    @PostMapping("/tables/{table}")
    public Map<String, Object> createRow(
            @PathVariable String table,
            @RequestBody Map<String, Object> row
    ) {
        validateTableName(table);
        
        // Удаляем id, если он есть (для автогенерации)
        row.remove("id");
        
        // Подготавливаем параметры
        MapSqlParameterSource params = new MapSqlParameterSource();
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        
        int i = 0;
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (i > 0) {
                columns.append(", ");
                values.append(", ");
            }
            String column = entry.getKey();
            Object value = entry.getValue();
            
            columns.append(column);
            values.append(":").append(column);
            params.addValue(column, convertValue(value));
            
            i++;
        }
        
        String sql = "INSERT INTO " + table + " (" + columns + ") VALUES (" + values + ") RETURNING id";
        
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbc.update(sql, params, keyHolder);
        
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("id", keyHolder.getKey());
        return result;
    }

    // Обновить запись в таблице
    @PutMapping("/tables/{table}/{id}")
    public Map<String, Object> updateRow(
            @PathVariable String table,
            @PathVariable Long id,
            @RequestBody Map<String, Object> row
    ) {
        validateTableName(table);
        
        // Проверяем, существует ли запись
        String checkSql = "SELECT COUNT(*) FROM " + table + " WHERE id = :id";
        MapSqlParameterSource checkParams = new MapSqlParameterSource();
        checkParams.addValue("id", id);
        Integer count = jdbc.queryForObject(checkSql, checkParams, Integer.class);
        
        if (count == null || count == 0) {
            throw new RuntimeException("Record with id " + id + " not found");
        }
        
        // Удаляем id из данных обновления
        row.remove("id");
        
        // Формируем SQL для UPDATE
        StringBuilder setClause = new StringBuilder();
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("id", id);
        
        int i = 0;
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (i > 0) {
                setClause.append(", ");
            }
            String column = entry.getKey();
            Object value = entry.getValue();
            
            setClause.append(column).append(" = :").append(column);
            params.addValue(column, convertValue(value));
            
            i++;
        }
        
        if (setClause.length() == 0) {
            // Нет полей для обновления
            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("message", "No fields to update");
            return result;
        }
        
        String sql = "UPDATE " + table + " SET " + setClause + " WHERE id = :id";
        
        int rowsAffected = jdbc.update(sql, params);
        
        Map<String, Object> result = new HashMap<>();
        result.put("success", rowsAffected > 0);
        result.put("rowsAffected", rowsAffected);
        return result;
    }

    // Удалить запись из таблицы
    @DeleteMapping("/tables/{table}/{id}")
    public Map<String, Object> deleteRow(@PathVariable String table, @PathVariable Long id) {
        validateTableName(table);
        
        String sql = "DELETE FROM " + table + " WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource("id", id);
        
        int rowsAffected = jdbc.update(sql, params);
        
        Map<String, Object> result = new HashMap<>();
        result.put("success", rowsAffected > 0);
        result.put("rowsAffected", rowsAffected);
        return result;
    }

    // Получить одну запись по ID
    @GetMapping("/tables/{table}/{id}")
    public Map<String, Object> getRowById(@PathVariable String table, @PathVariable Long id) {
        validateTableName(table);
        
        String sql = "SELECT * FROM " + table + " WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource("id", id);
        
        try {
            return jdbc.queryForMap(sql, params);
        } catch (EmptyResultDataAccessException e) {
            throw new RuntimeException("Record with id " + id + " not found");
        }
    }

    // Получение данных по внешнему ключу (ИСПРАВЛЕННЫЙ ВЕРСИЯ)
    @GetMapping("/tables/{table}/filter")
    public List<Map<String, Object>> getByFk(
            @PathVariable String table,
            @RequestParam String column,
            @RequestParam String value  // Оставляем String, так как из запроса всегда приходит строка
    ) {
        validateTableName(table);
        if (!column.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("Invalid column name: " + column);
        }
        
        // Создаем SQL-запрос с параметром
        String sql = "SELECT * FROM " + table + " WHERE " + column + " = :value ORDER BY id";
        MapSqlParameterSource params = new MapSqlParameterSource("value", convertValue(value));
        
        return jdbc.queryForList(sql, params);
    }

    // Отчеты для системы рудных месторождений
    @GetMapping("/reports/{report}")
    public List<Map<String, Object>> getReport(
            @PathVariable String report,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(required = false) Integer year,
            @RequestParam(required = false) Integer month,
            @RequestParam(required = false) String q,
            @RequestParam(required = false) Boolean isConfirmed
    ) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        
        String sql = switch (report) {
            // 1. Ежемесячная добыча по месторождениям
            case "monthly-production" -> {
                if (year != null && month != null) {
                    params.addValue("year", year);
                    params.addValue("month", month);
                    yield """
                        SELECT 
                            od.name AS deposit_name,
                            m.name AS mineral_name,
                            COUNT(ws.id) AS shifts_count,
                            COALESCE(SUM(sp.tons_of_ore), 0) AS total_production,
                            COALESCE(ROUND(AVG(sp.tons_of_ore), 2), 0) AS avg_production_per_shift
                        FROM work_shifts ws
                        JOIN ore_deposits od ON ws.ore_deposit_id = od.id
                        LEFT JOIN shift_production sp ON ws.id = sp.shift_id
                        LEFT JOIN minerals m ON sp.mineral_id = m.id
                        WHERE EXTRACT(YEAR FROM ws.shift_date) = :year
                          AND EXTRACT(MONTH FROM ws.shift_date) = :month
                        GROUP BY od.name, m.name
                        ORDER BY total_production DESC
                        """;
                } else if (year != null) {
                    params.addValue("year", year);
                    yield """
                        SELECT 
                            od.name AS deposit_name,
                            EXTRACT(MONTH FROM ws.shift_date) AS month,
                            COALESCE(SUM(sp.tons_of_ore), 0) AS total_production,
                            COUNT(ws.id) AS shifts_count
                        FROM work_shifts ws
                        JOIN ore_deposits od ON ws.ore_deposit_id = od.id
                        LEFT JOIN shift_production sp ON ws.id = sp.shift_id
                        WHERE EXTRACT(YEAR FROM ws.shift_date) = :year
                        GROUP BY od.name, EXTRACT(MONTH FROM ws.shift_date)
                        ORDER BY month, total_production DESC
                        """;
                } else {
                    yield """
                        SELECT 
                            od.name AS deposit_name,
                            m.name AS mineral_name,
                            EXTRACT(YEAR FROM ws.shift_date) AS year,
                            EXTRACT(MONTH FROM ws.shift_date) AS month_number,
                            COUNT(ws.id) AS shifts_count,
                            COALESCE(SUM(sp.tons_of_ore), 0) AS total_production,
                            COALESCE(ROUND(AVG(sp.tons_of_ore), 2), 0) AS avg_production
                        FROM work_shifts ws
                        JOIN ore_deposits od ON ws.ore_deposit_id = od.id
                        LEFT JOIN shift_production sp ON ws.id = sp.shift_id
                        LEFT JOIN minerals m ON sp.mineral_id = m.id
                        GROUP BY od.name, m.name, EXTRACT(YEAR FROM ws.shift_date), EXTRACT(MONTH FROM ws.shift_date)
                        ORDER BY year DESC, month_number DESC
                        """;
                }
            }

            // 2. Продажи по видам минералов
            case "sales-by-mineral" -> {
                if (from != null && to != null) {
                    params.addValue("from", from);
                    params.addValue("to", to);
                    yield """
                        SELECT 
                            m.name AS mineral_name,
                            COUNT(s.id) AS sales_count,
                            COALESCE(SUM(s.sold_tons), 0) AS total_sold_tons,
                            COALESCE(ROUND(AVG(s.sale_price_per_ton), 2), 0) AS avg_price,
                            COALESCE(SUM(s.sold_tons * s.sale_price_per_ton), 0) AS total_revenue,
                            MIN(s.sale_date) AS first_sale_date,
                            MAX(s.sale_date) AS last_sale_date
                        FROM sales_to_companies s
                        JOIN minerals m ON s.mineral_id = m.id
                        WHERE s.sale_date BETWEEN :from::date AND :to::date
                        GROUP BY m.name
                        ORDER BY total_revenue DESC
                        """;
                } else {
                    yield """
                        SELECT 
                            m.name AS mineral_name,
                            COUNT(s.id) AS sales_count,
                            COALESCE(SUM(s.sold_tons), 0) AS total_sold_tons,
                            COALESCE(ROUND(AVG(s.sale_price_per_ton), 2), 0) AS avg_price,
                            COALESCE(SUM(s.sold_tons * s.sale_price_per_ton), 0) AS total_revenue,
                            MIN(s.sale_date) AS first_sale_date,
                            MAX(s.sale_date) AS last_sale_date
                        FROM sales_to_companies s
                        JOIN minerals m ON s.mineral_id = m.id
                        GROUP BY m.name
                        ORDER BY total_revenue DESC
                        """;
                }
            }

            // 3. Статус запасов (подтвержденные/неподтвержденные)
            case "reserves-status" -> {
                if (isConfirmed != null) {
                    params.addValue("isConfirmed", isConfirmed);
                    yield """
                        SELECT 
                            od.name AS deposit_name,
                            m.name AS mineral_name,
                            r.absolute_volume,
                            r.is_confirmed
                        FROM reserves r
                        JOIN ore_deposits od ON r.ore_deposit_id = od.id
                        JOIN minerals m ON r.mineral_id = m.id
                        WHERE r.is_confirmed = :isConfirmed
                        ORDER BY od.name, m.name
                        """;
                } else {
                    yield """
                        SELECT 
                            od.name AS deposit_name,
                            m.name AS минерал,
                            r.absolute_volume,
                            r.is_confirmed
                        FROM reserves r
                        JOIN ore_deposits od ON r.ore_deposit_id = od.id
                        JOIN minerals m ON r.mineral_id = m.id
                        ORDER BY od.name, m.name
                        """;
                }
            }

            // 4. Отчет по инфраструктуре месторождений
            case "infrastructure-report" -> {
                if (q != null && !q.isBlank()) {
                    params.addValue("q", "%" + q + "%");
                    yield """
                        SELECT 
                            od.name AS deposit_name,
                            od.status,
                            od.has_railroad,
                            od.has_power_supply,
                            od.nearby_settlement
                        FROM ore_deposits od
                        WHERE od.name ILIKE :q
                           OR od.nearby_settlement ILIKE :q
                        ORDER BY od.name
                        """;
                } else {
                    yield """
                        SELECT 
                            od.name AS deposit_name,
                            od.status,
                            od.has_railroad,
                            od.has_power_supply,
                            od.nearby_settlement
                        FROM ore_deposits od
                        ORDER BY od.name
                        """;
                }
            }

            // 5. Эффективность работы горнодобывающих команд
            case "team-efficiency" -> {
                if (year != null && month != null) {
                    params.addValue("year", year);
                    params.addValue("month", month);
                    yield """
                        SELECT 
                            tn.name AS team_name,
                            mt.foreman_name,
                            COUNT(DISTINCT ws.shift_date) AS working_days,
                            COALESCE(SUM(sp.tons_of_ore), 0) AS total_production,
                            COALESCE(ROUND(SUM(sp.tons_of_ore) / NULLIF(COUNT(DISTINCT ws.shift_date), 0), 2), 0) AS avg_daily_production,
                            COALESCE(ROUND(AVG(sp.tons_of_ore), 2), 0) AS avg_shift_production,
                            SUM(CASE WHEN sp.equipment_damaged THEN 1 ELSE 0 END) AS incidents_count
                        FROM mining_teams mt
                        JOIN teams t ON mt.team_id = t.id
                        JOIN team_names tn ON t.name_id = tn.id
                        JOIN work_shifts ws ON mt.id = ws.mining_team_id
                        LEFT JOIN shift_production sp ON ws.id = sp.shift_id
                        WHERE EXTRACT(YEAR FROM ws.shift_date) = :year
                          AND EXTRACT(MONTH FROM ws.shift_date) = :month
                        GROUP BY tn.name, mt.foreman_name
                        ORDER BY total_production DESC
                        """;
                } else {
                    yield """
                        SELECT 
                            tn.name AS team_name,
                            mt.foreman_name,
                            EXTRACT(YEAR FROM ws.shift_date) AS year,
                            EXTRACT(MONTH FROM ws.shift_date) AS month,
                            COUNT(DISTINCT ws.shift_date) AS working_days,
                            COALESCE(SUM(sp.tons_of_ore), 0) AS total_production,
                            COALESCE(ROUND(SUM(sp.tons_of_ore) / NULLIF(COUNT(DISTINCT ws.shift_date), 0), 2), 0) AS avg_daily_production,
                            COALESCE(ROUND(AVG(sp.tons_of_ore), 2), 0) AS avg_shift_production,
                            SUM(CASE WHEN sp.equipment_damaged THEN 1 ELSE 0 END) AS incidents_count
                        FROM mining_teams mt
                        JOIN teams t ON mt.team_id = t.id
                        JOIN team_names tn ON t.name_id = tn.id
                        JOIN work_shifts ws ON mt.id = ws.mining_team_id
                        LEFT JOIN shift_production sp ON ws.id = sp.shift_id
                        GROUP BY tn.name, mt.foreman_name, EXTRACT(YEAR FROM ws.shift_date), EXTRACT(MONTH FROM ws.shift_date)
                        ORDER BY year DESC, month DESC, total_production DESC
                        """;
                }
            }

            // 6. Статистика по покупателям
            case "buyer-statistics" -> {
                if (from != null && to != null) {
                    params.addValue("from", from);
                    params.addValue("to", to);
                    yield """
                        SELECT 
                            bc.name AS company_name,
                            bc.contact_name,
                            bc.contact_phone,
                            COUNT(s.id) AS purchase_count,
                            COALESCE(SUM(s.sold_tons), 0) AS total_purchased_tons,
                            COALESCE(SUM(s.sold_tons * s.sale_price_per_ton), 0) AS total_spent,
                            COALESCE(ROUND(AVG(s.sale_price_per_ton), 2), 0) AS avg_price_paid,
                            MIN(s.sale_date) AS first_purchase_date,
                            MAX(s.sale_date) AS last_purchase_date
                        FROM buyers_companies bc
                        LEFT JOIN sales_to_companies s ON bc.id = s.buyer_id
                        WHERE (s.sale_date BETWEEN :from::date AND :to::date OR s.sale_date IS NULL)
                        GROUP BY bc.id, bc.name, bc.contact_name, bc.contact_phone
                        ORDER BY total_spent DESC NULLS LAST
                        """;
                } else {
                    yield """
                        SELECT 
                            bc.name AS company_name,
                            bc.contact_name,
                            bc.contact_phone,
                            COUNT(s.id) AS purchase_count,
                            COALESCE(SUM(s.sold_tons), 0) AS total_purchased_tons,
                            COALESCE(SUM(s.sold_tons * s.sale_price_per_ton), 0) AS total_spent,
                            COALESCE(ROUND(AVG(s.sale_price_per_ton), 2), 0) AS avg_price_paid,
                            MIN(s.sale_date) AS first_purchase_date,
                            MAX(s.sale_date) AS last_purchase_date
                        FROM buyers_companies bc
                        LEFT JOIN sales_to_companies s ON bc.id = s.buyer_id
                        GROUP BY bc.id, bc.name, bc.contact_name, bc.contact_phone
                        ORDER BY total_spent DESC NULLS LAST
                        """;
                }
            }

            // 7. Хронология открытия месторождений
            case "discovery-timeline" -> {
                if (from != null && to != null) {
                    params.addValue("from", from);
                    params.addValue("to", to);
                    yield """
                        SELECT 
                            od.discovery_year,
                            COUNT(*) AS deposits_discovered,
                            STRING_AGG(od.name, ', ') AS deposit_names
                        FROM ore_deposits od
                        WHERE od.discovery_year BETWEEN :from AND :to
                        GROUP BY od.discovery_year
                        ORDER BY od.discovery_year DESC
                        """;
                } else {
                    yield """
                        SELECT 
                            od.discovery_year,
                            COUNT(*) AS deposits_discovered,
                            STRING_AGG(od.name, ', ') AS deposit_names
                        FROM ore_deposits od
                        GROUP BY od.discovery_year
                        ORDER BY od.discovery_year DESC
                        """;
                }
            }

            // 8. Отчет о повреждениях оборудования
            case "equipment-damage" -> {
                if (from != null && to != null) {
                    params.addValue("from", from);
                    params.addValue("to", to);
                    yield """
                        SELECT 
                            ws.shift_date,
                            od.name AS deposit_name,
                            tn.name AS team_name,
                            mt.foreman_name,
                            sp.tons_of_ore,
                            sp.equipment_damaged,
                            sp.notes AS damage_description
                        FROM shift_production sp
                        JOIN work_shifts ws ON sp.shift_id = ws.id
                        JOIN ore_deposits od ON ws.ore_deposit_id = od.id
                        JOIN mining_teams mt ON ws.mining_team_id = mt.id
                        JOIN teams t ON mt.team_id = t.id
                        JOIN team_names tn ON t.name_id = tn.id
                        WHERE sp.equipment_damaged = true
                          AND ws.shift_date BETWEEN :from::date AND :to::date
                        ORDER BY ws.shift_date DESC
                        """;
                } else {
                    yield """
                        SELECT 
                            ws.shift_date,
                            od.name AS deposit_name,
                            tn.name AS team_name,
                            mt.foreman_name,
                            sp.tons_of_ore,
                            sp.equipment_damaged,
                            sp.notes AS damage_description
                        FROM shift_production sp
                        JOIN work_shifts ws ON sp.shift_id = ws.id
                        JOIN ore_deposits od ON ws.ore_deposit_id = od.id
                        JOIN mining_teams mt ON ws.mining_team_id = mt.id
                        JOIN teams t ON mt.team_id = t.id
                        JOIN team_names tn ON t.name_id = tn.id
                        WHERE sp.equipment_damaged = true
                        ORDER BY ws.shift_date DESC
                        """;
                }
            }

            default -> throw new IllegalArgumentException("Unknown report type: " + report);
        };

        return jdbc.queryForList(sql, params);
    }

    // Конвертация значений для SQL (ИСПРАВЛЕННАЯ ВЕРСИЯ)
    private Object convertValue(Object value) {
        if (value == null) {
            return null;
        }
        
        // Если это строка, пытаемся определить тип
        if (value instanceof String) {
            String str = ((String) value).trim();
            
            // Проверяем на булевы значения
            if ("true".equalsIgnoreCase(str)) {
                return true;
            } else if ("false".equalsIgnoreCase(str)) {
                return false;
            }
            
            // Пробуем конвертировать в Long
            try {
                return Long.parseLong(str);
            } catch (NumberFormatException e1) {
                // Пробуем конвертировать в Integer
                try {
                    return Integer.parseInt(str);
                } catch (NumberFormatException e2) {
                    // Пробуем конвертировать в Double
                    try {
                        return Double.parseDouble(str);
                    } catch (NumberFormatException e3) {
                        // Оставляем как строку
                        return str;
                    }
                }
            }
        }
        
        // Если это уже другой тип (Number, Boolean), возвращаем как есть
        return value;
    }
}