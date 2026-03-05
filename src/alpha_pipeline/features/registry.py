from __future__ import annotations

from alpha_pipeline.features.base import Feature, get_registered_features


class FeatureRegistry:
    """Manages feature lifecycle: registration, enable/disable, dependency ordering."""

    def __init__(self) -> None:
        self._features: dict[str, Feature] = {}
        self._enabled: set[str] = set()

    def load_registered(self) -> None:
        """Load all features from the decorator registry."""
        for name, cls in get_registered_features().items():
            self._features[name] = cls()
            self._enabled.add(name)

    def register(self, feature: Feature) -> None:
        name = feature.spec().name
        self._features[name] = feature
        self._enabled.add(name)

    def enable(self, name: str) -> None:
        if name not in self._features:
            raise KeyError(f"Feature '{name}' not registered")
        self._enabled.add(name)

    def disable(self, name: str) -> None:
        self._enabled.discard(name)

    def is_enabled(self, name: str) -> bool:
        return name in self._enabled

    def get_enabled(self) -> list[Feature]:
        """Return enabled features in topological order (respecting depends_on)."""
        return self._topo_sort([
            self._features[n] for n in self._enabled if n in self._features
        ])

    def get(self, name: str) -> Feature | None:
        return self._features.get(name)

    @property
    def all_names(self) -> list[str]:
        return list(self._features.keys())

    def _topo_sort(self, features: list[Feature]) -> list[Feature]:
        """Topological sort based on depends_on."""
        name_to_feat = {f.spec().name: f for f in features}
        visited: set[str] = set()
        result: list[Feature] = []

        def visit(name: str) -> None:
            if name in visited:
                return
            visited.add(name)
            feat = name_to_feat.get(name)
            if feat is None:
                return
            for dep in feat.spec().depends_on:
                visit(dep)
            result.append(feat)

        for f in features:
            visit(f.spec().name)

        return result
