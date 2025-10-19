import argparse

import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta


# Инициализация Faker
fake = Faker('ru_RU')


def populate_writers(connection, count):
    """Заполнение таблицы писателей"""
    # with - создает контекстный менеджер
    with connection.cursor() as cursor:
        for _ in range(count):
            first_name = fake.first_name()
            last_name = fake.last_name()

            cursor.execute(
                "INSERT INTO writers (first_name, last_name) VALUES (%s, %s)",
                (first_name, last_name)
            )
        connection.commit()
        print(f"Добавлено {count} писателей")


def populate_books(connection, count):
    """Заполнение таблицы книг"""
    # Присваивает созданный курсор переменной cursor.
    with connection.cursor() as cursor:
        # Получаем список всех writer_id
        cursor.execute("SELECT writer_id FROM writers")
        # каждая строка результата SQL-запроса возвращается как кортеж
        # Пример: [(1,), (2,), (3,), (5,), (8,)]
        # Поэтому берём везде 0 элемент
        writer_ids = [row[0] for row in cursor.fetchall()]

        # Проверка на наличие элементов
        if not writer_ids:
            print("Нет писателей в базе данных!")
            return

        # Значение не важно
        for _ in range(count):
            title = fake.catch_phrase()
            release_date = fake.date_between(start_date='-30y', end_date='today')
            writer_id = random.choice(writer_ids)

            cursor.execute(
                "INSERT INTO books (title, release_date, writer_id) VALUES (%s, %s, %s)",
                (title, release_date, writer_id)
            )

        connection.commit()
        print(f"Добавлено {count} книг")


def populate_readers(connection, count):
    """Заполнение таблицы читателей"""
    with connection.cursor() as cursor:
        for _ in range(count):
            # Генерируем отдельно имя и фамилию
            first_name = fake.first_name()
            last_name = fake.last_name()
            # Генерируем номер телефона
            phone_number = fake.phone_number()
            registration_date = fake.date_between(start_date='-5y', end_date='today')

            cursor.execute(
                "INSERT INTO readers (first_name, last_name, phone_number, registration_date) VALUES (%s, %s, %s, %s)",
                (first_name, last_name, phone_number, registration_date)
            )
        connection.commit()
        print(f"Добавлено {count} читателей")


def should_populate_data(connection):
    """Проверяет, есть ли уже данные в таблицах писателей, книг и читателей"""
    with connection.cursor() as cursor:
        # Проверяем все три таблицы
        cursor.execute("SELECT COUNT(*) FROM writers")
        writers_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM books")
        books_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM readers")
        readers_count = cursor.fetchone()[0]

        # Возвращаем True только если ВСЕ три таблицы пустые
        return writers_count == 0 and books_count == 0 and readers_count == 0


def display_random_readers_and_books(connection):
    """Вывод 20 случайных читателей и 20 случайных книг"""
    with connection.cursor() as cursor:
        # Вывод случайных читателей
        print("=" * 50)
        print("СЛУЧАЙНЫЕ ЧИТАТЕЛИ:")
        print("ID\tИмя\tФамилия")
        print("-" * 30)

        cursor.execute("""
            SELECT reader_id, first_name, last_name 
            FROM readers 
            ORDER BY RANDOM() 
            LIMIT 20
        """)
        readers = cursor.fetchall()

        for reader in readers:
            # Распаковка: Предполагается, что reader содержит три элемента в строгом порядке.
            reader_id, first_name, last_name = reader
            print(f"{reader_id}\t{first_name}\t{last_name}")

        # Вывод случайных книг
        print("\n" + "=" * 50)
        print("СЛУЧАЙНЫЕ КНИГИ:")
        print("ID\tНазвание книги")
        print("-" * 30)

        # Берём книги из таб. books и присоединяет к ним имя автора из таб. writers по связи ключей
        cursor.execute("""
            SELECT b.book_id, b.title, w.first_name, w.last_name 
            FROM books b 
            JOIN writers w ON b.writer_id = w.writer_id 
            ORDER BY RANDOM() 
            LIMIT 20
        """)
        books = cursor.fetchall()

        for book in books:
            book_id, title, author_first, author_last = book
            print(f"{book_id}\t{title} ({author_first} {author_last})")

        return readers, books


def add_book_rating(connection, reader_id, book_id, rating_value):
    """Добавление оценки книги читателем"""
    with connection.cursor() as cursor:
        try:
            # Проверяем существование читателя и книги
            cursor.execute("SELECT reader_id FROM readers WHERE reader_id = %s", (reader_id,))
            if not cursor.fetchone():
                print(f"Ошибка: Читатель с ID {reader_id} не найден")
                return False

            cursor.execute("SELECT book_id FROM books WHERE book_id = %s", (book_id,))
            if not cursor.fetchone():
                print(f"Ошибка: Книга с ID {book_id} не найден")
                return False

            # Проверяем, не оценивал ли уже этот читатель данную книгу
            cursor.execute(
                "SELECT rating_entry_id FROM book_ratings WHERE reader_id = %s AND book_id = %s",
                (reader_id, book_id)
            )
            if cursor.fetchone():
                print(f"Ошибка: Этот читатель уже оценивал данную книгу")
                return False
            # Проверяем существование такой оценки
            # Получаем rating_id по значению оценки
            cursor.execute("SELECT rating_id FROM ratings WHERE rating_value = %s", (rating_value,))
            rating_result = cursor.fetchone()

            if not rating_result:
                print(f"Ошибка: Некорректное значение оценки. Допустимые значения: 1-5")
                return False

            rating_id = rating_result[0]

            # Если всё прошло успешно
            # Добавляем оценку
            cursor.execute(
                """INSERT INTO book_ratings (reader_id, book_id, rating_id) 
                   VALUES (%s, %s, %s)""",
                (reader_id, book_id, rating_id)
            )
            connection.commit()

            print(f"✅ Оценка добавлена успешно!")
            print(f"   Читатель ID: {reader_id}")
            print(f"   Книга ID: {book_id}")
            print(f"   Оценка: {rating_value}/5")
            return True

        except psycopg2.Error as e:
            connection.rollback()
            print(f"Ошибка базы данных: {e}")
            return False


def show_recent_ratings(connection, limit=5):
    """Показать последние оценки"""
    # С лимитом вывода (5 строк)
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT br.rating_entry_id, r.first_name, r.last_name, b.title, 
                   rat.rating_value, br.rating_date
            FROM book_ratings br
            JOIN readers r ON br.reader_id = r.reader_id
            JOIN books b ON br.book_id = b.book_id
            JOIN ratings rat ON br.rating_id = rat.rating_id
            ORDER BY br.rating_date DESC
            LIMIT %s
        """, (limit,))

        ratings = cursor.fetchall()

        if ratings:
            print("\n" + "=" * 50)
            print("ПОСЛЕДНИЕ ОЦЕНКИ:")
            print("-" * 50)
            for rating in ratings:
                rating_id, first_name, last_name, title, rating_value, rating_date = rating
                print(f"{first_name} {last_name} -> '{title}' | Оценка: {rating_value}  | {rating_date}")
        else:
            print("\nПока нет оценок книг")


def get_book_rating_stats(connection, book_id=None):

    with connection.cursor() as cursor:
        if book_id is None:
            book_id = input("🎯 Введите ID книги: ").strip()
            if not book_id.isdigit():
                print("❌ Ошибка: введите числовой ID книги")
                return
            book_id = int(book_id)

        cursor.execute("""
            SELECT 
                b.title,
                ROUND(AVG(r.rating_value), 2) as avg_rating,
                COUNT(br.rating_id) as rating_count,
                MIN(r.rating_value) as min_rating,
                MAX(r.rating_value) as max_rating
            FROM books b
            LEFT JOIN book_ratings br ON b.book_id = br.book_id
            LEFT JOIN ratings r ON br.rating_id = r.rating_id
            WHERE b.book_id = %s
            GROUP BY b.book_id, b.title
        """, (book_id,))

        result = cursor.fetchone()

        if not result:
            print("❌ Книга не найдена!")
            return

        title, avg_rating, count, min_rating, max_rating = result

        print(f"\n📊 Статистика книги: \"{title}\"")

        if avg_rating is not None:
            print(f"   ⭐ Средний рейтинг: {avg_rating}/5")
            print(f"   ⭐ Количество оценок: {count}")
            print(f"   ⭐ Минимальная оценка: {min_rating}/5")
            print(f"   ⭐ Максимальная оценка: {max_rating}/5")

            # Визуальное отображение рейтинга
            full_stars = int(avg_rating)
            half_star = avg_rating - full_stars >= 0.5
            stars = "★" * full_stars + ("½" if half_star else "")
            print(f"   {stars}")
        else:
            print("   📝 У этой книги пока нет оценок")



def main():
    # Параметры подключения к PostgreSQL
    connection_params = {
        'host': 'localhost',
        'database': 'library_rating_system',
        'user': 'postgres',
        'password': 'your_password',
        'port': '5432'
    }

    connection = None

    try:
        # Подключение к базе данных
        connection = psycopg2.connect(**connection_params)
        print("Подключение к базе данных установлено")

        # Проверка и заполнение данных
        if should_populate_data(connection):
            print("Заполняем базу данных начальными данными...")
            populate_writers(connection, 1000)
            populate_books(connection, 1000)
            populate_readers(connection, 1000)
        else:
            print("Данные уже существуют в базе")

            # Парсинг аргументов командной строки
            parser = argparse.ArgumentParser(description='Система оценки книг')
            parser.add_argument('-reader', type=int, help='ID читателя')
            parser.add_argument('-book', type=int, help='ID книги')
            parser.add_argument('-rating', type=int, choices=[1, 2, 3, 4, 5], help='Оценка книги (1-5)')
            parser.add_argument('-stats', type=int, help='Показать статистику книги по ID')

            args = parser.parse_args()

            # Если аргументы не переданы, показываем случайных читателей и книги
            if not any(vars(args).values()):
                readers, books = display_random_readers_and_books(connection)
                show_recent_ratings(connection)

                print("\n" + "=" * 50)
                print("ДЛЯ ОЦЕНКИ КНИГИ ИСПОЛЬЗУЙТЕ КОМАНДУ:")
                print("python script.py -reader [ID_читателя] -book [ID_книги] -rating [1-5]")
                print("\nПРИМЕР:")
                if readers and books:
                    random_reader = random.choice(readers)
                    random_book = random.choice(books)
                    print(
                        f"python script.py -reader {random_reader[0]} -book {random_book[0]} -rating {random.randint(1, 5)}")

            # Если все аргументы переданы, создаем оценку
            elif args.reader and args.book and args.rating:
                success = add_book_rating(connection, args.reader, args.book, args.rating)
                if success:
                    show_recent_ratings(connection)

            elif args.stats:
                get_book_rating_stats(connection, args.stats)  # ← Вызов функции статистики

            else:
                print("Ошибка: Необходимо указать все три параметра: -reader, -book, -rating")
                print("Используйте --help для справки")

    except Exception as error:
        print(f"Ошибка: {error}")
    finally:
        if connection:
            connection.close()
            print("Соединение с базой данных закрыто")

if __name__ == "__main__":
    main()
    print("Программа завершена")