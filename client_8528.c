#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>

#define SERVER_IP "127.0.0.1"
#define PORT 14528
#define BUFFER_SIZE 4096

int client_socket;

void *receive_messages(void *arg)
{
    char buffer[BUFFER_SIZE];
    ssize_t bytes_received;

    (void)arg;

    while (1)
    {
        memset(buffer, 0, sizeof(buffer));

        bytes_received = recv(client_socket,
                              buffer,
                              sizeof(buffer) - 1,
                              0);

        if (bytes_received <= 0)
        {
            printf("\nDisconnected from server.\n");
            break;
        }

        buffer[bytes_received] = '\0';

        printf("\n%s", buffer);
        printf("> ");
        fflush(stdout);
    }

    return NULL;
}

int main(void)
{
    struct sockaddr_in server_addr;
    pthread_t receiver_thread;
    char username[100];
    char command[BUFFER_SIZE];

    client_socket = socket(AF_INET, SOCK_STREAM, 0);

    if (client_socket < 0)
    {
        perror("socket");
        return EXIT_FAILURE;
    }

    memset(&server_addr, 0, sizeof(server_addr));

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);

    if (inet_pton(AF_INET,
                  SERVER_IP,
                  &server_addr.sin_addr) <= 0)
    {
        perror("inet_pton");
        close(client_socket);
        return EXIT_FAILURE;
    }

    if (connect(client_socket,
                (struct sockaddr *)&server_addr,
                sizeof(server_addr)) < 0)
    {
        perror("connect");
        close(client_socket);
        return EXIT_FAILURE;
    }

    printf("Connected to NetMessenger server.\n");
    printf("Server: %s:%d\n", SERVER_IP, PORT);

    printf("Enter username: ");
    fflush(stdout);

    if (fgets(username, sizeof(username), stdin) == NULL)
    {
        close(client_socket);
        return EXIT_FAILURE;
    }

    username[strcspn(username, "\n")] = '\0';

    snprintf(command,
             sizeof(command),
             "REGISTER %s\n",
             username);

    if (send(client_socket,
             command,
             strlen(command),
             0) < 0)
    {
        perror("send");
        close(client_socket);
        return EXIT_FAILURE;
    }

    if (pthread_create(&receiver_thread,
                       NULL,
                       receive_messages,
                       NULL) != 0)
    {
        perror("pthread_create");
        close(client_socket);
        return EXIT_FAILURE;
    }

    pthread_detach(receiver_thread);

    printf("Type commands below.\n");
    printf("> ");
    fflush(stdout);

    while (1)
    {
        if (fgets(command, sizeof(command), stdin) == NULL)
        {
            break;
        }

        if (send(client_socket,
                 command,
                 strlen(command),
                 0) < 0)
        {
            perror("send");
            break;
        }

        if (strncmp(command, "QUIT", 4) == 0)
        {
            break;
        }

        printf("> ");
        fflush(stdout);
    }

    close(client_socket);

    return 0;
}
