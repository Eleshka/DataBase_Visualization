import psycopg2
import pandas as pd
import streamlit as st
from graphviz import Digraph
import matplotlib.pyplot as plt
import networkx as nx

def get_postgres_schema_with_relations(
    host="158.160.185.213", 
    port=5432, 
    database="demo", 
    user="postgres", 
    password="123"
):
    """
    Получает полную структуру БД: таблицы, колонки, связи (FK), индексы.
    """
    conn = psycopg2.connect(
        host=host, port=port, database=database, 
        user=user, password=password
    )
    cursor = conn.cursor()
    
    # 1. Получаем все таблицы
    cursor.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
          AND table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name;
    """)
    tables = cursor.fetchall()
    
    schema_info = {
        'tables': [],
        'columns': {},
        'primary_keys': {},
        'foreign_keys': [],
        'indexes': {}
    }
    
    for schema, table in tables:
        table_full = f"{schema}.{table}"
        schema_info['tables'].append(table_full)
        
        # 2. Получаем колонки
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """, (schema, table))
        
        columns = cursor.fetchall()
        schema_info['columns'][table_full] = [
            {'name': col[0], 'type': col[1], 'nullable': col[2], 'position': col[3]}
            for col in columns
        ]
        
        # 3. Получаем первичные ключи
        cursor.execute("""
            SELECT c.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage ccu 
                ON tc.constraint_name = ccu.constraint_name
            JOIN information_schema.columns c 
                ON c.table_schema = tc.table_schema 
                AND c.table_name = tc.table_name 
                AND c.column_name = ccu.column_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = %s
                AND tc.table_name = %s;
        """, (schema, table))
        
        pk_columns = cursor.fetchall()
        schema_info['primary_keys'][table_full] = [pk[0] for pk in pk_columns]
        
        # 4. Получаем внешние ключи (связи между таблицами)
        cursor.execute("""
            SELECT
                kcu.column_name,
                ccu.table_schema AS foreign_schema,
                ccu.table_name AS foreign_table,
                ccu.column_name AS foreign_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = %s
                AND tc.table_name = %s;
        """, (schema, table))
        
        fks = cursor.fetchall()
        for fk in fks:
            schema_info['foreign_keys'].append({
                'from_table': f"{schema}.{table}",
                'from_column': fk[0],
                'to_table': f"{fk[1]}.{fk[2]}",
                'to_column': fk[3]
            })
        
        # 5. Получаем индексы
        cursor.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = %s AND tablename = %s;
        """, (schema, table))
        
        indexes = cursor.fetchall()
        schema_info['indexes'][table_full] = [
            {'name': idx[0], 'definition': idx[1]} for idx in indexes
        ]
    
    conn.close()
    return schema_info

def generate_erd_graphviz(schema_info):
    """
    Генерирует ER-диаграмму с помощью Graphviz.
    Установка: pip install graphviz
    """
    dot = Digraph(comment='Database Schema')
    dot.attr(rankdir='LR', size='12,8')
    
    # Добавляем таблицы
    for table in schema_info['tables']:
        # Формируем label таблицы с колонками
        label = f"<<TABLE BORDER='1' CELLBORDER='1' CELLSPACING='0'>"
        label += f"<TR><TD COLSPAN='2' BGCOLOR='lightblue'><B>{table}</B></TD></TR>"
        
        for col in schema_info['columns'][table]:
            # Отмечаем первичные ключи
            if table in schema_info['primary_keys'] and col['name'] in schema_info['primary_keys'][table]:
                col_name = f"🔑 {col['name']}"
                bgcolor = 'lightgreen'
            else:
                col_name = col['name']
                bgcolor = 'white'
            
            label += f"<TR><TD BGCOLOR='{bgcolor}'>{col_name}</TD><TD>{col['type']}</TD></TR>"
        
        label += "</TABLE>>"
        
        dot.node(table, label=label, shape='plaintext')
    
    # Добавляем связи (FK)
    for fk in schema_info['foreign_keys']:
        dot.edge(fk['from_table'], fk['to_table'], 
                label=f"{fk['from_column']} → {fk['to_column']}",
                color='blue', fontsize='10')
    
    return dot

def generate_erd_networkx(schema_info):
    """
    Генерирует граф связей с помощью NetworkX и Matplotlib.
    Установка: pip install networkx matplotlib
    """
    G = nx.DiGraph()
    
    # Добавляем узлы-таблицы
    for table in schema_info['tables']:
        # Размер узла зависит от количества колонок
        node_size = len(schema_info['columns'].get(table, [])) * 300
        G.add_node(table, size=len(schema_info['columns'][table]) * 100)
    
    # Добавляем ребра-связи
    edge_labels = {}
    for fk in schema_info.get('foreign_keys', []):
        from_table = fk['from_table']
        to_table = fk['to_table']
        G.add_edge(from_table, to_table)
        edge_labels[(from_table, to_table)] = f"{fk['from_column']} → {fk['to_column']}"
    
     
    # Рисуем граф
    fig, ax = plt.subplots(figsize=(20, 14))

    # Позиционирование узлов
    pos = nx.spring_layout(G, k=3, iterations=50, seed=42)

    # Размеры узлов
    node_sizes = [G.nodes[n].get('size', 2000) for n in G.nodes]
    
    # Рисуем узлы
    nx.draw_networkx_nodes(G, pos, 
                              node_color='lightblue',
                              node_size=node_sizes,
                              alpha=0.9,
                              ax=ax)
    
   # Рисуем ребра
    nx.draw_networkx_edges(G, pos,
                              edge_color='gray',
                              width=2,
                              arrows=True,
                              arrowsize=25,
                              arrowstyle='->',
                              ax=ax)
    
    # Рисуем подписи узлов (названия таблиц)
    nx.draw_networkx_labels(G, pos,
                               font_size=12,
                               font_weight='bold',
                               ax=ax)
    
    # Рисуем подписи ребер (связи)
    if edge_labels:
        nx.draw_networkx_edge_labels(G, pos,
                                    edge_labels=edge_labels,
                                    font_size=10,
                                    font_color='blue',
                                    bbox=dict(boxstyle='round,pad=0.3',
                                                fc='yellow',
                                                alpha=0.7),
                                    ax=ax)
    
    
    ax.set_title("Граф связей базы данных", 
                    fontsize=20, 
                    fontweight='bold',
                    pad=20)
        
    ax.axis('off')
    plt.tight_layout()
    
    return fig
# ============ STREAMLIT ВИЗУАЛИЗАЦИЯ ============
# Установка: pip install streamlit pandas matplotlib networkx graphviz

def streamlit_db_visualizer():
    """
    Функция для Streamlit - визуализация структуры БД
    """
    st.set_page_config(page_title="Database Schema Visualizer", layout="wide")
   

    st.title("🗄️ Database Schema Visualization")
    st.markdown("---")
    
    # Боковая панель с настройками
    with st.sidebar:
        st.header("🔧 Настройки подключения")
        
        host = st.text_input("Host", "158.160.185.213")
        port = st.number_input("Port", 5432)
        database = st.text_input("Database", "demo")
        user = st.text_input("User", "postgres")
        password = st.text_input("Password", "123", type="password")
        
        if st.button("🔄 Загрузить схему", type="primary"):
            with st.spinner("Загрузка структуры базы данных..."):
                try:
                    schema_info = get_postgres_schema_with_relations(
                        host, port, database, user, password
                    )
                    st.session_state['schema_info'] = schema_info
                    st.success(f"✅ Загружено {len(schema_info['tables'])} таблиц")
                    st.info(f"🔗 Найдено {len(schema_info.get('foreign_keys', []))} связей")
                except Exception as e:
                    st.error(f"❌ Ошибка: {e}")
    
    # Основной контент
    if 'schema_info' in st.session_state:
        schema_info = st.session_state['schema_info']
        
        # Табы для разных видов визуализации
        tab1, tab2, tab3, tab4 = st.tabs(
            ["ER-диаграмма", "Граф связей", "Таблицы", "Анализ"]
        )
        
        with tab1:
            st.subheader("ER-диаграмма (Graphviz)")
            
            if st.button("🔄 Сгенерировать ER-диаграмму"):
                try:
                    dot = generate_erd_graphviz(schema_info)
                    st.graphviz_chart(dot.source)
                except Exception as e:
                    st.error(f"Ошибка генерации: {e}")
                    st.info("Установите graphviz: pip install graphviz")
        
        with tab2:
            st.subheader("Граф связей (NetworkX)")
            
            if st.button("🔄 Сгенерировать граф"):
                try:
                    fig = generate_erd_networkx(schema_info)
                    st.pyplot(fig)
                except Exception as e:
                    st.error(f"Ошибка генерации: {e}")
        
        with tab3:
            st.subheader("Детальная структура таблиц")
            
            for table in schema_info['tables']:
                with st.expander(f"📁 {table}"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**Колонки:**")
                        df_columns = pd.DataFrame(schema_info['columns'][table])
                        st.dataframe(df_columns, use_container_width=True)
                    
                    with col2:
                        st.markdown("**Первичные ключи:**")
                        if table in schema_info['primary_keys']:
                            st.write(", ".join(schema_info['primary_keys'][table]))
                        else:
                            st.write("Нет")
                        
                        st.markdown("**Индексы:**")
                        if table in schema_info['indexes']:
                            for idx in schema_info['indexes'][table]:
                                st.code(idx['definition'])
        
        with tab4:
            st.subheader("Анализ связей")
            
            st.metric("Всего таблиц", len(schema_info['tables']))
            st.metric("Всего связей (FK)", len(schema_info['foreign_keys']))
            
            st.markdown("**Внешние ключи:**")
            if schema_info['foreign_keys']:
                df_fk = pd.DataFrame(schema_info['foreign_keys'])
                st.dataframe(df_fk, use_container_width=True)
            
            # Статистика по таблицам
            st.markdown("**Статистика по таблицам:**")
            stats = []
            for table in schema_info['tables']:
                stats.append({
                    'Таблица': table,
                    'Колонок': len(schema_info['columns'][table]),
                    'PK': len(schema_info['primary_keys'].get(table, [])),
                    'FK': sum(1 for fk in schema_info['foreign_keys'] 
                            if fk['from_table'] == table),
                    'Индексов': len(schema_info['indexes'].get(table, []))
                })
            
            df_stats = pd.DataFrame(stats)
            st.dataframe(df_stats, use_container_width=True)
    else:
        st.info("👈 Нажмите 'Загрузить схему' в боковой панели для начала работы")

# Для запуска Streamlit:
if __name__ == "__main__":
    # streamlit run этот_файл.py
    streamlit_db_visualizer()