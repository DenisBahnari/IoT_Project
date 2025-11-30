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


def analyze_cluster_profiles(conn):
    """
    Analisa as características de cada cluster KMeans baseado em SESSÕES
    Retorna: quantidade de sessões e métricas agregadas por cluster
    """
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        # Query focada em SESSÕES por cluster
        query = """
        WITH session_metrics AS (
            SELECT 
                evs.cluster_kmeans,
                
                -- Contagem de sessões
                COUNT(*) as total_sessions,
                
                -- Energia e custos (agregados por sessão)
                AVG(evs.energy_consumed_kwh) as avg_energy_per_session,
                SUM(evs.energy_consumed_kwh) as total_energy_consumed,
                AVG(evs.charging_cost_eur) as avg_cost_per_session,
                SUM(evs.charging_cost_eur) as total_cost,
                
                -- Duração e taxa de carregamento
                AVG(evs.duration_h) as avg_duration,
                AVG(evs.charging_rate_kw) as avg_charging_rate,
                
                -- Período preferido (baseado em sessões)
                MODE() WITHIN GROUP (ORDER BY evs.time_of_day) as most_common_time,
                MODE() WITHIN GROUP (ORDER BY evs.day_of_week) as most_common_day,
                
                -- Distribuição geográfica (contagem de estações únicas)
                COUNT(DISTINCT evs.station_id) as unique_stations_used,
                COUNT(DISTINCT est.distrito) as unique_districts,
                
                -- Informações do veículo (médias por sessão)
                AVG(evs.battery_capacity_kwh) as avg_battery_capacity,
                AVG(evs.vehicle_age_years) as avg_vehicle_age,
                AVG(evs.distance_driven_km) as avg_distance_driven,
                
                -- Estado de carga
                AVG(evs.soc_start) as avg_soc_start,
                AVG(evs.soc_end) as avg_soc_end,
                
                -- Temperatura
                AVG(evs.temperature_c) as avg_temperature,
                
                -- Horário médio de carregamento
                AVG(EXTRACT(HOUR FROM evs.start_time)) as avg_start_hour,
                
                -- Utilizadores únicos (apenas para contexto)
                COUNT(DISTINCT evs.user_id) as unique_users

            FROM ev_session evs
            LEFT JOIN ev_station est ON evs.station_id = est.station_id
            WHERE evs.cluster_kmeans IS NOT NULL 
            GROUP BY evs.cluster_kmeans
        ),
        time_distribution AS (
            SELECT 
                cluster_kmeans,
                time_of_day,
                COUNT(*) as session_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY cluster_kmeans), 2) as percentage
            FROM ev_session
            WHERE cluster_kmeans IS NOT NULL AND time_of_day IS NOT NULL
            GROUP BY cluster_kmeans, time_of_day
        ),
        vehicle_models AS (
            SELECT 
                cluster_kmeans,
                vehicle_model,
                COUNT(*) as model_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY cluster_kmeans), 2) as model_percentage
            FROM ev_session
            WHERE cluster_kmeans IS NOT NULL AND vehicle_model IS NOT NULL
            GROUP BY cluster_kmeans, vehicle_model
        )
        
        SELECT 
            sm.*,
            td.time_of_day,
            td.percentage as time_percentage,
            vm.vehicle_model as most_common_vehicle,
            vm.model_percentage as vehicle_percentage,
            
            -- Perfil baseado nas características das SESSÕES
            CASE 
                WHEN sm.avg_duration < 2 AND sm.most_common_time IN ('Morning', 'Afternoon') THEN 'Quick Day Chargers'
                WHEN sm.avg_energy_per_session > 40 AND sm.avg_duration > 3 THEN 'Long Session Users'
                WHEN sm.most_common_time = 'Night' THEN 'Overnight Chargers'
                WHEN sm.avg_charging_rate > 10 THEN 'Fast Charging Sessions'
                WHEN sm.unique_stations_used > 5 THEN 'Multi-Station Users'
                ELSE 'Standard Usage'
            END as session_profile

        FROM session_metrics sm
        LEFT JOIN time_distribution td ON sm.cluster_kmeans = td.cluster_kmeans 
            AND td.session_count = (
                SELECT MAX(session_count) 
                FROM time_distribution td2 
                WHERE td2.cluster_kmeans = sm.cluster_kmeans
            )
        LEFT JOIN (
            SELECT DISTINCT ON (cluster_kmeans) 
                cluster_kmeans, vehicle_model, model_percentage
            FROM vehicle_models 
            ORDER BY cluster_kmeans, model_count DESC
        ) vm ON sm.cluster_kmeans = vm.cluster_kmeans
        ORDER BY sm.cluster_kmeans;
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Processar resultados - foco em SESSÕES
        cluster_analysis = {}
        for row in results:
            cluster_id = row['cluster_kmeans']
            
            cluster_analysis[cluster_id] = {
                'cluster_id': cluster_id,
                'session_count': row['total_sessions'],
                'unique_users': row['unique_users'],
                'session_profile': row['session_profile'],
                'metrics': {
                    'energy': {
                        'avg_energy_per_session': round(row['avg_energy_per_session'], 2),
                        'total_energy_consumed': round(row['total_energy_consumed'], 2)
                    },
                    'cost': {
                        'avg_cost_per_session': round(row['avg_cost_per_session'], 4),
                        'total_cost': round(row['total_cost'], 2)
                    },
                    'duration': {
                        'avg_duration': round(row['avg_duration'], 2),
                        'avg_charging_rate': round(row['avg_charging_rate'], 2)
                    },
                    'geographic': {
                        'unique_stations_used': row['unique_stations_used'],
                        'unique_districts': row['unique_districts']
                    },
                    'vehicle': {
                        'avg_battery_capacity': round(row['avg_battery_capacity'], 2),
                        'avg_vehicle_age': round(row['avg_vehicle_age'], 1),
                        'avg_distance_driven': round(row['avg_distance_driven'], 2),
                        'most_common_vehicle': row['most_common_vehicle'],
                        'vehicle_percentage': row['vehicle_percentage']
                    },
                    'battery': {
                        'avg_soc_start': round(row['avg_soc_start'], 1),
                        'avg_soc_end': round(row['avg_soc_end'], 1)
                    },
                    'temporal': {
                        'most_common_time': row['most_common_time'],
                        'most_common_day': row['most_common_day'],
                        'avg_start_hour': round(row['avg_start_hour'], 1),
                        'peak_time_percentage': row['time_percentage']
                    }
                }
            }
        
        return {
            'success': True,
            'cluster_analysis': cluster_analysis,
            'total_clusters': len(cluster_analysis),
            'summary': {
                'total_sessions_analyzed': sum(cluster['session_count'] for cluster in cluster_analysis.values()),
                'total_energy_consumed': sum(cluster['metrics']['energy']['total_energy_consumed'] for cluster in cluster_analysis.values()),
                'total_cost': sum(cluster['metrics']['cost']['total_cost'] for cluster in cluster_analysis.values()),
                'most_common_profile': max(cluster_analysis.values(), key=lambda x: x['session_count'])['session_profile']
            }
        }
        
    except Exception as e:
        print(f"Error analyzing clusters: {e}")
        return {'success': False, 'error': str(e)}
    finally:
        cursor.close()


def get_user_clusters(conn):
    cursor = conn.cursor()

    cursor.execute("""
        SELECT user_id, cluster_kmeans AS most_common_cluster
        FROM (
            SELECT 
                user_id, 
                cluster_kmeans,
                COUNT(*) AS cluster_count,
                ROW_NUMBER() OVER (
                    PARTITION BY user_id 
                    ORDER BY COUNT(*) DESC
                ) AS rn
            FROM ev_session
            WHERE cluster_kmeans IS NOT NULL
            GROUP BY user_id, cluster_kmeans
        ) sub
        WHERE rn = 1
        ORDER BY user_id;
    """)

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    # Converte para um dicionário (opcional)
    result = {user_id: cluster for user_id, cluster in rows}
    return result
