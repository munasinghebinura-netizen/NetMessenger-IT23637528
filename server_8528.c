#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <time.h>

#define PORT 14528
#define NID "6378"
#define BACKLOG 10
#define MAX_CLIENTS 50
#define USERNAME_SIZE 50
#define BUFFER_SIZE 4096

#define LOG_FILE "netmsg_IT236378528.log"

typedef struct
{
    int socket;
    char username[USERNAME_SIZE];
    int registered;
} Client;

Client clients[MAX_CLIENTS];
#define MAX_ROOMS 50
#define MAX_ROOM_MEMBERS 50
#define ROOM_NAME_SIZE 50

typedef struct {
    char name[ROOM_NAME_SIZE];
    int members[MAX_ROOM_MEMBERS];
    int member_count;
} Room;

Room rooms[MAX_ROOMS];
int room_count = 0;

pthread_mutex_t rooms_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

void write_log(const char *message)
{
    pthread_mutex_lock(&log_mutex);

    FILE *fp = fopen(LOG_FILE, "a");

    if (fp != NULL)
    {
        time_t now = time(NULL);
        struct tm *t = localtime(&now);

        fprintf(fp,
                "[%04d-%02d-%02d %02d:%02d:%02d] %s\n",
                t->tm_year + 1900,
                t->tm_mon + 1,
                t->tm_mday,
                t->tm_hour,
                t->tm_min,
                t->tm_sec,
                message);

        fclose(fp);
    }

    pthread_mutex_unlock(&log_mutex);
}

void send_response(int socket, const char *message)
{
    char response[BUFFER_SIZE];

    snprintf(response,
             sizeof(response),
             "%s NID:%s\n",
             message,
             NID);

    send(socket, response, strlen(response), 0);
}

int username_exists(const char *username)
{
    int exists = 0;

    pthread_mutex_lock(&clients_mutex);

    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].registered &&
            strcmp(clients[i].username, username) == 0)
        {
            exists = 1;
            break;
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    return exists;
}

int add_client(int socket, const char *username)
{
    int result = -1;

    pthread_mutex_lock(&clients_mutex);

    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (!clients[i].registered)
        {
            clients[i].socket = socket;

            strncpy(clients[i].username,
                    username,
                    USERNAME_SIZE - 1);

            clients[i].username[USERNAME_SIZE - 1] = '\0';
            clients[i].registered = 1;

            result = i;
            break;
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    return result;
}

void remove_client(int socket)
{
    pthread_mutex_lock(&clients_mutex);

    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].registered &&
            clients[i].socket == socket)
        {
            clients[i].registered = 0;
            clients[i].socket = -1;
            clients[i].username[0] = '\0';
            break;
        }
    }

    pthread_mutex_unlock(&clients_mutex);
}void send_to_all(const char *message, int sender_socket)
{
    pthread_mutex_lock(&clients_mutex);

    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].registered &&
            clients[i].socket != sender_socket)
        {
            send(clients[i].socket,
                 message,
                 strlen(message),
                 0);
        }
    }

    pthread_mutex_unlock(&clients_mutex);
}
int find_client_socket(const char *username)
{
    int socket = -1;

    pthread_mutex_lock(&clients_mutex);

    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].registered &&
            strcmp(clients[i].username, username) == 0)
        {
            socket = clients[i].socket;
            break;
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    return socket;
}

void *client_handler(void *arg)
{
    int client_socket = *(int *)arg;
    free(arg);

    char buffer[BUFFER_SIZE];
    char username[USERNAME_SIZE];

    int registered = 0;

    printf("Client connected.\n");

    write_log("Client connected");

    while (1)
    {
        memset(buffer, 0, sizeof(buffer));

        ssize_t bytes_received =
            recv(client_socket,
                 buffer,
                 sizeof(buffer) - 1,
                 0);

        if (bytes_received <= 0)
        {
            break;
        }

        buffer[bytes_received] = '\0';

        char *newline = strchr(buffer, '\n');

        if (newline != NULL)
        {
            *newline = '\0';
        }

        buffer[strcspn(buffer, "\r")] = '\0';

        if (!registered)
        {
            if (strncmp(buffer, "REGISTER ", 9) != 0)
            {
                send_response(client_socket,
                              "ERR 001 REGISTER_REQUIRED");
                continue;
            }

            char requested_username[USERNAME_SIZE];

            strncpy(requested_username,
                    buffer + 9,
                    USERNAME_SIZE - 1);

            requested_username[USERNAME_SIZE - 1] = '\0';

            if (strlen(requested_username) == 0)
            {
                send_response(client_socket,
                              "ERR 001 INVALID_USERNAME");
                continue;
            }

            if (username_exists(requested_username))
            {
                send_response(client_socket,
                              "ERR 001 USERNAME_TAKEN");
                continue;
            }

            if (add_client(client_socket,
                           requested_username) < 0)
            {
                send_response(client_socket,
                              "ERR 005 SERVER_FULL");
                continue;
            }

            strcpy(username, requested_username);
            registered = 1;

            char response[BUFFER_SIZE];

            snprintf(response,
                     sizeof(response),
                     "OK REGISTERED %s",
                     username);

            send_response(client_socket, response);

            char log_message[BUFFER_SIZE];

            snprintf(log_message,
                     sizeof(log_message),
                     "User registered: %s",
                     username);

            write_log(log_message);

            printf("User registered: %s\n", username);

            continue;
        }        if (strcmp(buffer, "LIST") == 0)
        {
            char response[BUFFER_SIZE];
            char users[BUFFER_SIZE];

            users[0] = '\0';

            pthread_mutex_lock(&clients_mutex);

            for (int i = 0; i < MAX_CLIENTS; i++)
            {
                if (clients[i].registered)
                {
                    if (strlen(users) > 0)
                    {
                        strcat(users, ",");
                    }

                    strcat(users, clients[i].username);
                }
            }

            pthread_mutex_unlock(&clients_mutex);

            snprintf(response,
                     sizeof(response),
                     "OK USERS %s",
                     users);

            send_response(client_socket, response);

            continue;
        }             if (strncmp(buffer, "PMSG ", 5) == 0)
        {
            char target[USERNAME_SIZE];
            char message[BUFFER_SIZE];
            char forwarded[BUFFER_SIZE];

            if (sscanf(buffer + 5,
                       "%49s %[^\n]",
                       target,
                       message) < 2)
            {
                send_response(client_socket,
                              "ERR 002 USER_NOT_FOUND");
                continue;
            }

            int target_socket = find_client_socket(target);

            if (target_socket < 0)
            {
                send_response(client_socket,
                              "ERR 002 USER_NOT_FOUND");
                continue;
            }

            snprintf(forwarded,
                     sizeof(forwarded),
                     "MSG PRIV %s %s\n",
                     username,
                     message);

            send(target_socket,
                 forwarded,
                 strlen(forwarded),
                 0);

            send_response(client_socket, "OK SENT");

            continue;
        }        






if (strncmp(buffer, "JOIN ", 5) == 0)
        {
            char room_name[ROOM_NAME_SIZE];

            if (sscanf(buffer + 5, "%49[^\n]", room_name) != 1)
            {
                send_response(client_socket, "ERR 003 ROOM_NOT_FOUND");
                continue;
            }

            pthread_mutex_lock(&rooms_mutex);

            int room_index = -1;

            for (int i = 0; i < room_count; i++)
            {
                if (strcmp(rooms[i].name, room_name) == 0)
                {
                    room_index = i;
                    break;
                }
            }

            /* Create room if it does not exist */
            if (room_index == -1)
            {
                if (room_count >= MAX_ROOMS)
                {
                    pthread_mutex_unlock(&rooms_mutex);
                    send_response(client_socket, "ERR 003 ROOM_NOT_FOUND");
                    continue;
                }

                room_index = room_count;

                strncpy(rooms[room_index].name,
                        room_name,
                        ROOM_NAME_SIZE - 1);

                rooms[room_index].name[ROOM_NAME_SIZE - 1] = '\0';
                rooms[room_index].member_count = 0;
                room_count++;
            }

            /* Check whether client is already a member */
            int already_member = 0;

            for (int i = 0;
                 i < rooms[room_index].member_count;
                 i++)
            {
                if (rooms[room_index].members[i] == client_socket)
                {
                    already_member = 1;
                    break;
                }
            }

            if (!already_member)
            {
                if (rooms[room_index].member_count >= MAX_ROOM_MEMBERS)
                {
                    pthread_mutex_unlock(&rooms_mutex);
                    send_response(client_socket, "ERR 003 ROOM_NOT_FOUND");
                    continue;
                }

                rooms[room_index].members[
                    rooms[room_index].member_count
                ] = client_socket;

                rooms[room_index].member_count++;
            }

            pthread_mutex_unlock(&rooms_mutex);

         char response[BUFFER_SIZE];
snprintf(response, sizeof(response), "OK JOINED %s", room_name);
send_response(client_socket, response);
            continue;
        }        if (strncmp(buffer, "LEAVE ", 6) == 0)
        {
            char room_name[ROOM_NAME_SIZE];

            if (sscanf(buffer + 6, "%49[^\n]", room_name) != 1)
            {
                send_response(client_socket, "ERR 003 ROOM_NOT_FOUND");
                continue;
            }

            pthread_mutex_lock(&rooms_mutex);

            int room_index = -1;

            for (int i = 0; i < room_count; i++)
            {
                if (strcmp(rooms[i].name, room_name) == 0)
                {
                    room_index = i;
                    break;
                }
            }

            if (room_index == -1)
            {
                pthread_mutex_unlock(&rooms_mutex);
                send_response(client_socket, "ERR 003 ROOM_NOT_FOUND");
                continue;
            }

            int member_found = 0;

            for (int i = 0; i < rooms[room_index].member_count; i++)
            {
                if (rooms[room_index].members[i] == client_socket)
                {
                    for (int j = i;
                         j < rooms[room_index].member_count - 1;
                         j++)
                    {
                        rooms[room_index].members[j] =
                            rooms[room_index].members[j + 1];
                    }

                    rooms[room_index].member_count--;
                    member_found = 1;
                    break;
                }
            }

            pthread_mutex_unlock(&rooms_mutex);

            if (member_found)
            {
                char response[BUFFER_SIZE];
                snprintf(response, sizeof(response),
                         "OK LEFT %s", room_name);
                send_response(client_socket, response);
            }
            else
            {
                send_response(client_socket, "ERR 003 ROOM_NOT_FOUND");
            }

            continue;
        }        if (strncmp(buffer, "ROOMS", 5) == 0)
        {
            char room_list[BUFFER_SIZE];
            room_list[0] = '\0';

            pthread_mutex_lock(&rooms_mutex);

            for (int i = 0; i < room_count; i++)
            {
                if (i > 0)
                {
                    strncat(room_list, ",",
                            sizeof(room_list) - strlen(room_list) - 1);
                }

                strncat(room_list,
                        rooms[i].name,
                        sizeof(room_list) - strlen(room_list) - 1);
            }

            pthread_mutex_unlock(&rooms_mutex);

            char response[BUFFER_SIZE];

            if (room_count == 0)
            {
                snprintf(response, sizeof(response), "OK ROOMS");
            }
            else
            {
                snprintf(response, sizeof(response),
                         "OK ROOMS %s", room_list);
            }

            send_response(client_socket, response);
            continue;
        }


   if (strncmp(buffer, "BCAST ", 6) == 0)
        {
            char message[BUFFER_SIZE];
            char forwarded[BUFFER_SIZE];

            snprintf(message,
                     sizeof(message),
                     "%s",
                     buffer + 6);

            snprintf(forwarded,
                     sizeof(forwarded),
                     "MSG BCAST %s %s\n",
                     username,
                     message);

            send_to_all(forwarded, client_socket);

            send_response(client_socket, "OK SENT");

            continue;
        }

        if (strcmp(buffer, "QUIT") == 0)
        {
            send_response(client_socket, "OK BYE");
            break;
        }

        send_response(client_socket,
                      "ERR 000 UNKNOWN_COMMAND");
    }

    if (registered)
    {
        char log_message[BUFFER_SIZE];

        snprintf(log_message,
                 sizeof(log_message),
                 "User disconnected: %s",
                 username);

        write_log(log_message);

        printf("User disconnected: %s\n",
               username);
    }

    remove_client(client_socket);

    close(client_socket);

    return NULL;
}

int main(void)
{
    int server_socket;

    struct sockaddr_in server_addr;

    memset(clients, 0, sizeof(clients));

    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        clients[i].socket = -1;
        clients[i].registered = 0;
    }

    server_socket =
        socket(AF_INET,
               SOCK_STREAM,
               0);

    if (server_socket < 0)
    {
        perror("socket");
        return EXIT_FAILURE;
    }

    int option = 1;

    if (setsockopt(server_socket,
                   SOL_SOCKET,
                   SO_REUSEADDR,
                   &option,
                   sizeof(option)) < 0)
    {
        perror("setsockopt");
        close(server_socket);
        return EXIT_FAILURE;
    }

    memset(&server_addr, 0, sizeof(server_addr));

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    if (bind(server_socket,
             (struct sockaddr *)&server_addr,
             sizeof(server_addr)) < 0)
    {
        perror("bind");
        close(server_socket);
        return EXIT_FAILURE;
    }

    if (listen(server_socket, BACKLOG) < 0)
    {
        perror("listen");
        close(server_socket);
        return EXIT_FAILURE;
    }

    printf("NetMessenger Server started\n");
    printf("Port: %d\n", PORT);
    printf("NID: %s\n", NID);
    printf("Waiting for clients...\n");

    write_log("NetMessenger server started");

    while (1)
    {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int *client_socket =
            malloc(sizeof(int));

        if (client_socket == NULL)
        {
            perror("malloc");
            continue;
        }

        *client_socket =
            accept(server_socket,
                   (struct sockaddr *)&client_addr,
                   &client_len);

        if (*client_socket < 0)
        {
            perror("accept");
            free(client_socket);
            continue;
        }

        pthread_t thread_id;

        if (pthread_create(&thread_id,
                           NULL,
                           client_handler,
                           client_socket) != 0)
        {
            perror("pthread_create");
            close(*client_socket);
            free(client_socket);
            continue;
        }

        pthread_detach(thread_id);
    }

    close(server_socket);

    return 0;
}
