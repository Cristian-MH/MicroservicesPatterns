# Build stage
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy only the csproj first
COPY ["InventoryService.csproj", "./"]
RUN dotnet restore "InventoryService.csproj"

# Now copy the rest of the source
COPY . .
RUN dotnet publish "InventoryService.csproj" -c Release -o /app/publish --no-restore

# Runtime stage
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
EXPOSE 8080
ENTRYPOINT ["dotnet", "InventoryService.dll"]
