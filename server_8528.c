

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <time.h>
#include <sys/stat.h>
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
void create_storage_directory(const char *username)
{
    char path[BUFFER_SIZE];

    mkdir("storage", 0777);
    mkdir("storage/IT236378528", 0777);

    snprintf(path, sizeof(path),
             "storage/IT236378528/%s",
             username);

    mkdir(path, 0777);
}
int receive_file_data(int socket,
                      char *pending,
                      size_t *pending_len,
                      size_t filesize,
                      FILE *file)
{
    size_t total_received = 0;

    while (total_received < filesize)
    {
        if (*pending_len > 0)
        {
            size_t available = *pending_len;
            size_t remaining = filesize - total_received;
            size_t to_write = available < remaining
                              ? available
                              : remaining;

            if (fwrite(pending, 1, to_write, file) != to_write)
            {
                return -1;
            }

            total_received += to_write;

            memmove(pending,
                    pending + to_write,
                    *pending_len - to_write);

            *pending_len -= to_write;

            continue;
        }

        char file_buffer[BUFFER_SIZE];

        size_t remaining = filesize - total_received;
        size_t to_read = remaining < sizeof(file_buffer)
                         ? remaining
                         : sizeof(file_buffer);

        ssize_t received = recv(socket,
                                 file_buffer,
                                 to_read,
                                 0);

        if (received <= 0)
        {
            return -1;
        }

        if (fwrite(file_buffer, 1, received, file) != (size_t)received)
        {
            return -1;
        }

        total_received += received;
    }

    return 0;
}
int receive_line(int socket,
                 char *line,
                 size_t line_size,
                 char *pending,
                 size_t *pending_len)
{
    while (1)
    {
        for (size_t i = 0; i < *pending_len; i++)
        {
            if (pending[i] == '\n')
            {
                size_t length = i + 1;

                if (length >= line_size)
                {
                    return -1;
                }

                memcpy(line, pending, length);
                line[length] = '\0';

                memmove(pending,
                        pending + length,
                        *pending_len - length);

                *pending_len -= length;

                return 1;
            }
        }

        if (*pending_len >= BUFFER_SIZE)
        {
            return -1;
        }

        ssize_t received = recv(socket,
                                 pending + *pending_len,
                                 BUFFER_SIZE - *pending_len,
                                 0);

        if (received <= 0)
        {
            return 0;
        }

        *pending_len += received;
    }
}
int send_all_bytes(int socket, const char *data, size_t length)
{
    size_t total_sent = 0;

    while (total_sent < length)
    {
        ssize_t sent = send(socket,
                            data + total_sent,
                            length - total_sent,
                            0);

        if (sent <= 0)
        {
            return -1;
        }

        total_sent += (size_t)sent;
    }

    return 0;
}
void *client_handler(void *arg)
{
    int client_socket = *(int *)arg;
    free(arg);

    char buffer[BUFFER_SIZE];
char pending[BUFFER_SIZE];
size_t pending_len = 0;
    char username[USERNAME_SIZE];

    int registered = 0;

    printf("Client connected.\n");

    write_log("Client connected");

    while (1)
    {
        memset(buffer, 0, sizeof(buffer));
        int line_status = receive_line(client_socket,
                                       buffer,
                                       sizeof(buffer),
                                       pending,
                                       &pending_len);

        if (line_status <= 0)
        {
            break;
        }

        buffer[strcspn(buffer, "\r\n")] = '\0';

      

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
create_storage_directory(username);

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
        }        if (strncmp(buffer, "RMSG ", 5) == 0)
        {
            char room_name[ROOM_NAME_SIZE];
            char message[BUFFER_SIZE];
            char forwarded[BUFFER_SIZE];

            if (sscanf(buffer + 5, "%49s %[^\n]",
                       room_name, message) < 2)
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

            snprintf(forwarded,
                     sizeof(forwarded),
                     "MSG ROOM %s %s %s\n",
                     room_name,
                     username,
                     message);

            for (int i = 0;
                 i < rooms[room_index].member_count;
                 i++)
            {
                int member_socket = rooms[room_index].members[i];

                send(member_socket,
                     forwarded,
                     strlen(forwarded),
                     0);
            }

            pthread_mutex_unlock(&rooms_mutex);

            send_response(client_socket, "OK SENT");
            continue;
        }        if (strncmp(buffer, "SENDFILE ", 9) == 0)
        {
            char target[USERNAME_SIZE];
            char filename[256];
            long filesize;

            if (sscanf(buffer + 9, "%49s %255s %ld",
                       target, filename, &filesize) != 3)
            {
                send_response(client_socket,
                              "ERR 004 FILE_TOO_LARGE");
                continue;
            }

            if (filesize <= 0)
            {
                send_response(client_socket,
                              "ERR 004 FILE_TOO_LARGE");
                continue;
            }

            printf("SENDFILE request: %s -> %s (%ld bytes)\n",
                   username, target, filesize);
                         int target_socket = find_client_socket(target);

        if (target_socket < 0)
        {
            send_response(client_socket,
                          "ERR 002 USER_NOT_FOUND");
            continue;
        }
                           char filepath[BUFFER_SIZE];
        char sender_directory[BUFFER_SIZE];

        snprintf(sender_directory,
                 sizeof(sender_directory),
                 "storage/IT236378528/%s",
                 username);

        snprintf(filepath,
                 sizeof(filepath),
                 "%s/%s",
                 sender_directory,
                 filename);
                     FILE *file = fopen(filepath, "wb");

        if (file == NULL)
        {
            send_response(client_socket,
                          "ERR 004 FILE_TOO_LARGE");
            continue;
        }

              if (receive_file_data(client_socket,
                              pending,
                              &pending_len,
                              (size_t)filesize,
                              file) != 0)
        {
            fclose(file);
            remove(filepath);
            break;
        }
             fclose(file);
                     char file_header[BUFFER_SIZE];

        snprintf(file_header,
                 sizeof(file_header),
                 "FILE %s %ld\n",
                 filename,
                 filesize);

        if (send_all_bytes(target_socket,
                           file_header,
                           strlen(file_header)) != 0)
        {
            printf("Failed to send file header to %s\n", target);
        }
        else
        {
            FILE *send_file = fopen(filepath, "rb");

            if (send_file == NULL)
            {
                printf("Failed to reopen file for sending\n");
            }
            else
            {
                char send_buffer[BUFFER_SIZE];
                size_t bytes_read;

                while ((bytes_read =
                        fread(send_buffer,
                              1,
                              sizeof(send_buffer),
                              send_file)) > 0)
                {
                    if (send_all_bytes(target_socket,
                                       send_buffer,
                                       bytes_read) != 0)
                    {
                        printf("Failed to send file to %s\n",
                               target);
                        break;
                    }
                }

                fclose(send_file);

                printf("File forwarded: %s -> %s (%ld bytes)\n",
                       username,
                       target,
                       filesize);
            }
        }
                     char response[BUFFER_SIZE];

        snprintf(response,
                 sizeof(response),
                 "OK FILE_RECEIVED %s",
                 filename);

        send_response(client_socket, response);

        printf("File received: %s (%ld bytes)\n",
               filepath,
               filesize);

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
