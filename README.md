# Electric Vehicle Charging Infrastructure Management

## Group Number 7

| Name           | Number |
|----------------|--------|
| Denis Bahnari  | 59878  |

---

## Project Description

(Descreve aqui o objetivo, funcionalidades e arquitetura do projeto.)

---

## Compilation & Run Instructions

### 🚀 Run the full project (backend + services)
Para iniciar todos os serviços via Docker, executar na **root do projeto**:

```bash
docker compose up --build
```

Para simular sessões enviadas por um cliente Python, executar:

```bash
python .\client_1\client_simulation.py
```

## Util Docker Commands:

docker compose down -v
docker system prune -a
docker network prune
docker volume prune

docker system prune -a --volumes -f