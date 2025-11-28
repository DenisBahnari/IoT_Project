from psycopg2.extras import RealDictCursor # type: ignore

def get_daily_weekly_monthly_trends(conn):
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        # Daily trends query
        daily_query = """
        SELECT 
            DATE(start_time) as date,
            COUNT(*) as session_count,
            SUM(energy_consumed_kwh) as total_energy,
            AVG(energy_consumed_kwh) as avg_energy,
            AVG(duration_h) as avg_duration,
            SUM(charging_cost_eur) as total_cost
        FROM ev_session 
        WHERE start_time IS NOT NULL
        GROUP BY DATE(start_time)
        ORDER BY date;
        """
        
        cursor.execute(daily_query)
        daily_trends = cursor.fetchall()
    
        # Weekly trends query
        weekly_query = """
        SELECT 
            EXTRACT(YEAR FROM start_time) as year,
            EXTRACT(WEEK FROM start_time) as week,
            COUNT(*) as session_count,
            SUM(energy_consumed_kwh) as total_energy,
            AVG(energy_consumed_kwh) as avg_energy,
            AVG(duration_h) as avg_duration,
            SUM(charging_cost_eur) as total_cost
        FROM ev_session 
        WHERE start_time IS NOT NULL
        GROUP BY EXTRACT(YEAR FROM start_time), EXTRACT(WEEK FROM start_time)
        ORDER BY year, week;
        """
        
        cursor.execute(weekly_query)
        weekly_trends = cursor.fetchall()
        
        # Monthly trends query
        monthly_query = """
        SELECT 
            EXTRACT(YEAR FROM start_time) as year,
            EXTRACT(MONTH FROM start_time) as month,
            COUNT(*) as session_count,
            SUM(energy_consumed_kwh) as total_energy,
            AVG(energy_consumed_kwh) as avg_energy,
            AVG(duration_h) as avg_duration,
            SUM(charging_cost_eur) as total_cost
        FROM ev_session 
        WHERE start_time IS NOT NULL
        GROUP BY EXTRACT(YEAR FROM start_time), EXTRACT(MONTH FROM start_time)
        ORDER BY year, month;
        """
        
        cursor.execute(monthly_query)
        monthly_trends = cursor.fetchall()
        
        result = {
            "daily_trends": daily_trends,
            "weekly_trends": weekly_trends,
            "monthly_trends": monthly_trends,
            "summary": {
                "total_sessions": sum(day['session_count'] for day in daily_trends),
                "total_energy": sum(day['total_energy'] or 0 for day in daily_trends),
                "total_cost": sum(day['total_cost'] or 0 for day in daily_trends)
            }
        }
        
        return result
        
    except Exception as e:
        print(f"Error calculating trends: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()


def get_time_of_day_distribution(conn):
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        query = """
        SELECT 
            time_of_day,
            COUNT(*) as session_count
        FROM ev_session 
        WHERE time_of_day IS NOT NULL
        GROUP BY time_of_day
        ORDER BY 
            CASE time_of_day
                WHEN 'morning' THEN 1
                WHEN 'afternoon' THEN 2
                WHEN 'evening' THEN 3
                WHEN 'night' THEN 4
            END;
        """
        
        cursor.execute(query)
        time_distribution = cursor.fetchall()
        
        return time_distribution
        
    except Exception as e:
        print(f"Error calculating time distribution: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()


def get_user_behavior_patterns(conn):
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        # Query 1: Consumo Mensal de Energia por User
        energy_query = """
        SELECT 
            user_id,
            COUNT(*) as total_sessions,
            SUM(energy_consumed_kwh) as total_energy_kwh,
            COUNT(DISTINCT DATE_TRUNC('month', start_time)) as months_active,
            CASE 
                WHEN COUNT(DISTINCT DATE_TRUNC('month', start_time)) > 0 
                THEN SUM(energy_consumed_kwh) / COUNT(DISTINCT DATE_TRUNC('month', start_time))
                ELSE SUM(energy_consumed_kwh)
            END as avg_monthly_energy_kwh,
            MIN(start_time) as first_session,
            MAX(start_time) as last_session
        FROM ev_session 
        WHERE user_id IS NOT NULL 
          AND energy_consumed_kwh IS NOT NULL
          AND start_time IS NOT NULL
        GROUP BY user_id
        ORDER BY avg_monthly_energy_kwh DESC;
        """
        
        cursor.execute(energy_query)
        energy_patterns = cursor.fetchall()
        
        # Query 2: Frequência de Utilização por User
        frequency_query = """
        SELECT 
            user_id,
            COUNT(*) as total_sessions,
            COUNT(DISTINCT DATE(start_time)) as unique_days_used,
            COUNT(DISTINCT DATE_TRUNC('month', start_time)) as months_active,
            CASE 
                WHEN COUNT(DISTINCT DATE_TRUNC('month', start_time)) > 0 
                THEN COUNT(*)::FLOAT / COUNT(DISTINCT DATE_TRUNC('month', start_time))
                ELSE COUNT(*)::FLOAT
            END as sessions_per_month,
            CASE 
                WHEN COUNT(DISTINCT DATE(start_time)) > 0 
                THEN COUNT(*)::FLOAT / COUNT(DISTINCT DATE(start_time))
                ELSE 1
            END as sessions_per_day_avg,
            MIN(start_time) as first_session,
            MAX(start_time) as last_session
        FROM ev_session 
        WHERE user_id IS NOT NULL 
          AND start_time IS NOT NULL
        GROUP BY user_id
        ORDER BY sessions_per_month DESC;
        """
        
        cursor.execute(frequency_query)
        frequency_patterns = cursor.fetchall()
        
        # Query 3: Quantidade de Estações Diferentes por User
        stations_query = """
        SELECT 
            user_id,
            COUNT(*) as total_sessions,
            COUNT(DISTINCT station_id) as unique_stations_used,
            CASE 
                WHEN COUNT(*) > 0 
                THEN COUNT(DISTINCT station_id)::FLOAT / COUNT(*)
                ELSE 0 
            END as station_variety_ratio,
            ARRAY_AGG(DISTINCT station_id) as stations_list,
            (SELECT station_id 
             FROM ev_session es2 
             WHERE es2.user_id = ev_session.user_id 
             GROUP BY station_id 
             ORDER BY COUNT(*) DESC 
             LIMIT 1) as preferred_station
        FROM ev_session 
        WHERE user_id IS NOT NULL 
          AND station_id IS NOT NULL
        GROUP BY user_id
        ORDER BY unique_stations_used DESC, total_sessions DESC;
        """
        
        cursor.execute(stations_query)
        stations_patterns = cursor.fetchall()
        
        # Combinar os resultados
        result = {
            "energy_consumption": energy_patterns,
            "usage_frequency": frequency_patterns,
            "station_mobility": stations_patterns,
            "summary": {
                "total_users": len(energy_patterns),
                "total_sessions": sum(user['total_sessions'] for user in energy_patterns),
                "total_energy_kwh": sum(user['total_energy_kwh'] or 0 for user in energy_patterns),
                "avg_sessions_per_user": len(energy_patterns) > 0 and sum(user['total_sessions'] for user in energy_patterns) / len(energy_patterns) or 0
            }
        }
        
        return result
        
    except Exception as e:
        print(f"Error calculating user behavior patterns: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()