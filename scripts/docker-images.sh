#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Uso: scripts/docker-images.sh [--push] TAG [APLICACAO...]

Constrói e executa --version em linux/amd64 e linux/arm64.
Sem aplicações na linha de comando, usa todos os módulos go-*.
Com --push, envia a tag testada ao Docker Hub (tmsoftbrasil).
EOF
}

fail() {
    printf 'Erro: %s\n' "$*" >&2
    exit 1
}

push=false
case "${1:-}" in
    -h|--help) usage; exit 0 ;;
    --push) push=true; shift ;;
esac

tag=${1:-}
[[ $tag =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$ ]] || {
    usage >&2
    fail 'informe uma tag Docker válida.'
}
shift

cd "$(dirname "${BASH_SOURCE[0]}")/.."
commit=$(git rev-parse HEAD)
namespace=docker.io/tmsoftbrasil
platforms=(linux/amd64 linux/arm64)

if [[ $# -eq 0 ]]; then
    for module in go-*/go.mod; do
        set -- "$@" "${module%/go.mod}"
    done
fi

images=()
for app in "$@"; do
    if [[ ! $app =~ ^go-[a-z0-9-]+$ || ! -f $app/go.mod ||
          ! -f $app/docker/Dockerfile || ! -f $app/cmd/$app/main.go ]]; then
        fail "aplicação inválida: $app"
    fi
    images+=("$namespace/$app:$tag")
done

for app in "$@"; do
    image="$namespace/$app:$tag"
    docker buildx build --platform "${platforms[0]},${platforms[1]}" --load \
        --build-arg "VERSION=$tag" --build-arg "COMMIT=$commit" \
        --tag "$image" --file "$app/docker/Dockerfile" "$app"
    for platform in "${platforms[@]}"; do
        printf '\n%s (%s)\n' "$image" "$platform"
        docker run --rm --pull=never --network=none --platform "$platform" "$image" --version
    done
done

if "$push"; then
    for image in "${images[@]}"; do
        docker image push "$image"
        published=$(docker buildx imagetools inspect --format \
            '{{range .Manifest.Manifests}}{{with .Platform}}{{println .OS "/" .Architecture}}{{end}}{{end}}' "$image")
        for platform in "${platforms[@]}"; do
            grep -Fxq "${platform%/*} / ${platform#*/}" <<< "$published" ||
                fail "$image não contém $platform no Docker Hub."
        done
    done
fi
