import json
import pytds
from decimal import Decimal
from datetime import date, datetime

SERVER = 'database-1.cde20wy6o0s0.us-east-2.rds.amazonaws.com'
DATABASE = 'DB_CURSOS'
USERNAME = 'admin'
PASSWORD = 'Admin123456.'
PORT = 1433

def lambda_handler(event, context):
    method = event.get("httpMethod") or event.get("requestContext", {}).get("http", {}).get("method")
    path = event.get("path") or event.get("rawPath", "")
    body = json.loads(event.get("body") or "{}")

    conn = pytds.connect(server=SERVER, database=DATABASE, user=USERNAME, password=PASSWORD, port=PORT)
    cursor = conn.cursor()

    if method == "POST" and path == "/students":
        return create_item(cursor, conn, "students", body)
    elif method == "POST" and path == "/courses":
        return create_item(cursor, conn, "courses", body)
    elif method == "POST" and path == "/enrollments":
        return create_item(cursor, conn, "enrollments", body)
    elif method == "PUT" and path.startswith("/enrollments/"):
        enrollment_id = path.split("/")[-1]
        return update_enrollment(cursor, conn, enrollment_id, body)
    elif method == "GET" and path.startswith("/students/") and path.endswith("/enrollments"):
        student_id = path.split("/")[2]
        return get_student_enrollments(cursor, student_id)
    else:
        return response(404, {"message": f"Ruta no encontrada: {method} {path}"})

def convert_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    return obj

def convert_value(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (date, datetime)):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    return obj

def get_student_enrollments(cursor, student_id):
    if not student_id:
        return response(400, {"message": "Debe especificar el 'id' del estudiante"})

    sql = f"""
        SELECT e.id, c.titulo, c.descripcion, e.estado, e.puntaje, e.fecha_matricula
        FROM enrollments e
        INNER JOIN courses c ON e.course_id = c.id
        WHERE e.student_id = {student_id}
    """

    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        items = [{desc[0]: convert_value(row[i]) for i, desc in enumerate(cursor.description)} for row in rows]
        return response(200, items)
    except Exception as e:
        return response(500, {"message": "Error al obtener item(s)", "error": str(e)})

def create_item(cursor, conn, tabla, datos):
    try:
        if tabla == "students":
            nombre = str(datos.get("nombre") or "")
            correo = str(datos.get("correo") or "")
            sql = f"INSERT INTO students (nombre, correo) VALUES ('{nombre}', '{correo}')"

        elif tabla == "courses":
            titulo = str(datos.get("titulo") or "")
            descripcion = str(datos.get("descripcion") or "")
            sql = f"INSERT INTO courses (titulo, descripcion) VALUES ('{titulo}', '{descripcion}')"

        elif tabla == "enrollments":
            student_id = int(datos.get("student_id") or 0)
            course_id = int(datos.get("course_id") or 0)
            sql = f"INSERT INTO enrollments (student_id, course_id, estado, puntaje) " \
                  f"VALUES ({student_id}, {course_id}, 'Activo', 100)"
        else:
            return response(400, {"message": f"Tabla no reconocida: {tabla}"})

        cursor.execute(sql)
        conn.commit()
        return response(201, {"message": f"Item creado en {tabla}"})

    except Exception as e:
        return response(500, {"message": "Error al crear item", "error": str(e)})


def update_enrollment(cursor, conn, enrollment_id, datos):
    if not enrollment_id:
        return response(400, {"message": "Debe especificar 'id' para actualizar"})

    estado = str(datos.get("estado") or "Activo")
    
    if estado not in ["Activo", "Inactivo"]:
        return response(400, {"message": "El estado debe ser 'Activo' o 'Inactivo'"})

    puntaje = str(datos.get("puntaje") or 0)
    sql = f"UPDATE enrollments SET estado='{estado}', puntaje='{puntaje}' WHERE id={enrollment_id}"

    try:
        cursor.execute(sql)
        conn.commit()
        return response(200, {"message": f"Item actualizado enrollments", "id": enrollment_id})
    except Exception as e:
        return response(500, {"message": "Error al actualizar item", "error": str(e)})


def response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body)
    }
